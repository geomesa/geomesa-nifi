package org.locationtech.geomesa.nifi

import java.io.InputStream
import java.util

import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnScheduled, OnStopped}
import org.apache.nifi.components.{ValidationResult, ValidationContext, PropertyDescriptor}
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.{FeatureWriter, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.convert
import org.locationtech.geomesa.convert.{ConverterConfigResolver, ConverterConfigLoader, SimpleFeatureConverters}
import org.locationtech.geomesa.nifi.AbstractGeoMesa._
import org.locationtech.geomesa.nifi.GeoMesaIngest._
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SimpleFeatureTypeLoader}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "convert", "accumulo"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
class GeoMesaIngest extends AbstractGeoMesa {

  type SFW = FeatureWriter[SimpleFeatureType, SimpleFeature]

  protected override def init(context: ProcessorInitializationContext): Unit = {
    descriptors = List(
      GeoMesaConfigController,
      Zookeepers,
      InstanceName,
      User,
      Password,
      Catalog,
      SftName,
      ConverterName,
      FeatureNameOverride,
      SftSpec,
      ConverterSpec
     ).asJava

    relationships = Set(SuccessRelationship, FailureRelationship).asJava
  }


  @volatile
  private var featureWriter: SFW = null

  @volatile
  private var converter: convert.SimpleFeatureConverter[_] = null

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    if (useControllerService){
       dataStore = getGeomesaControllerService(context).getDataStore()
    }
    else {
      dataStore = getDataStore(context)
    }

    val sft = getSft(context)
    dataStore.createSchema(sft)

    converter = getConverter(sft, context)
    featureWriter = createFeatureWriter(sft, context)
    getLogger.info(s"Initialized GeoMesaIngest datastore, fw, converter for type ${sft.getTypeName}")
  }

  @OnStopped
  def cleanup(): Unit = {
    IOUtils.closeQuietly(featureWriter)
    featureWriter = null
    dataStore = null
    getLogger.info("Shut down GeoMesaIngest processor " + getIdentifier)
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
    Option(session.get()).foreach(doWork(context, session, _))

  private def doWork(context: ProcessContext, session: ProcessSession, flowFile: FlowFile): Unit = {
    try {
      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          val fullFlowFileName = flowFile.getAttribute("path") + flowFile.getAttribute("filename")
          getLogger.info(s"Converting path $fullFlowFileName")
          val ec = converter.createEvaluationContext(Map("inputFilePath" -> fullFlowFileName))
          converter
            .process(in, ec)
            .foreach { sf =>
              val toWrite = featureWriter.next()
              toWrite.setAttributes(sf.getAttributes)
              toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
              toWrite.getUserData.putAll(sf.getUserData)
              featureWriter.write()
            }
        }
      })
      session.transfer(flowFile, SuccessRelationship)
    } catch {
      case e: Exception =>
        getLogger.error("error", e)
        session.transfer(flowFile, FailureRelationship)
    }
  }

  private def getSft(context: ProcessContext): SimpleFeatureType = {
    val sftArg = Option(context.getProperty(SftName).getValue)
      .orElse(Option(context.getProperty(SftSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${SftName.getName} or ${SftSpec.getName} property"))
    val typeName = context.getProperty(FeatureNameOverride).getValue
    SftArgResolver.getSft(sftArg, typeName).getOrElse(throw new IllegalArgumentException(s"Could not resolve sft from config value $sftArg and typename $typeName"))
  }

  private def createFeatureWriter(sft: SimpleFeatureType, context: ProcessContext): SFW = {
    dataStore.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
  }

  private def getConverter(sft: SimpleFeatureType, context: ProcessContext): convert.SimpleFeatureConverter[_] = {
    val convertArg = Option(context.getProperty(ConverterName).getValue)
      .orElse(Option(context.getProperty(ConverterSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${ConverterName.getName} or ${ConverterSpec.getName} property"))
    val config = ConverterConfigResolver.getConfig(convertArg).get
    SimpleFeatureConverters.build(sft, config)
  }

 override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {

   val validationFailures: java.util.Collection[ValidationResult] = new util.ArrayList[ValidationResult]()

   val controllerSet:Boolean =  validationContext.getProperty(GeoMesaConfigController).isSet
   useControllerService = controllerSet

   val zooSet:Boolean =         validationContext.getProperty(Zookeepers).isSet
   val instanceSet: Boolean =   validationContext.getProperty(InstanceName).isSet
   val userSet: Boolean =       validationContext.getProperty(User).isSet
   val passSet: Boolean =       validationContext.getProperty(Password).isSet
   val catalogSet: Boolean =    validationContext.getProperty(Catalog).isSet

   // require either controller-service or all of {zoo,instance,user,pw,catalog}
   if(!controllerSet && !(zooSet && instanceSet && userSet && passSet && catalogSet))
     validationFailures.add(new ValidationResult.Builder()
       .input("Use either GeoMesa Configuration Service, or specify accumulo connection parameters.")
       .build)

   // make sure either a sft is named or written
   val sftNameSet: Boolean = validationContext.getProperty(SftName).isSet
   val sftSpecSet: Boolean = validationContext.getProperty(SftSpec).isSet
   if(!sftNameSet && !sftSpecSet)
     validationFailures.add(new ValidationResult.Builder()
     .input("Specify a simple feature type by name or spec")
     .build)

   // make sure either a converter is named or written
   val convNameSet: Boolean = validationContext.getProperty(ConverterName).isSet
   val convSpecSet: Boolean = validationContext.getProperty(ConverterSpec).isSet
   if(!convNameSet && !convSpecSet)
     validationFailures.add(new ValidationResult.Builder()
        .input("Specify a converter by name or spec")
        .build
     )

   validationFailures
  }

}

object GeoMesaIngest {
  val SftName = new PropertyDescriptor.Builder()
    .name("SftName")
    .description("Choose a simple feature type defined by a GeoMesa SFT Provider (preferred)")
    .required(false)
    .allowableValues(SimpleFeatureTypeLoader.listTypeNames.toArray: _*)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val ConverterName = new PropertyDescriptor.Builder()
    .name("ConverterName")
    .description("Choose an SimpleFeature Converter defined by a GeoMesa SFT Provider (preferred)")
    .required(false)
    .allowableValues(ConverterConfigLoader.listConverterNames.toArray: _*)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)   // TODO validate
    .build

  val FeatureNameOverride = new PropertyDescriptor.Builder()
    .name("FeatureNameOverride")
    .description("Override the Simple Feature Type name from the SFT Spec")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)  // TODO validate
    .build

  val SftSpec = new PropertyDescriptor.Builder()
    .name("SftSpec")
    .description("Manually define a SimpleFeatureType (SFT) config spec")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val ConverterSpec = new PropertyDescriptor.Builder()
    .name("ConverterSpec")
    .description("Manually define a converter using typesafe config")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

}