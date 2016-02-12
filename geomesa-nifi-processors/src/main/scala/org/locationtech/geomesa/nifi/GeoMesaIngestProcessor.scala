package org.locationtech.geomesa.nifi

import java.io.InputStream

import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnScheduled, OnStopped}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.{DataStore, DataStoreFinder, FeatureWriter, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.convert
import org.locationtech.geomesa.convert.{ConverterConfigResolver, ConverterConfigLoader, SimpleFeatureConverters}
import org.locationtech.geomesa.nifi.GeoMesaIngestProcessor._
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SimpleFeatureTypeLoader}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "convert"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
class GeoMesaIngestProcessor extends AbstractProcessor {

  type SFW = FeatureWriter[SimpleFeatureType, SimpleFeature]

  private var descriptors: java.util.List[PropertyDescriptor] = null
  private var relationships: java.util.Set[Relationship] = null

  protected override def init(context: ProcessorInitializationContext): Unit = {
    descriptors = List(
      Zookeepers,
      InstanceName,
      User,
      Password,
      Catalog,
      SftName,
      ConverterName,
      FeatureName,
      SftSpec,
      ConverterSpec
     ).asJava

    relationships = Set(SuccessRelationship, FailureRelationship).asJava
  }

  @volatile
  private var featureWriter: SFW = null

  @volatile
  private var dataStore: DataStore = null

  @volatile
  private var converter: convert.SimpleFeatureConverter[_] = null


  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    dataStore = getDataStore(context)
    val sft = getSft(context)
    dataStore.createSchema(sft)

    converter = getConverter(sft, context)
    featureWriter = createFeatureWriter(sft, context)
    getLogger.info(s"Initialized GeoMesaIngestProcessor datastore, fw, converter for type ${sft.getTypeName}")
  }

  @OnStopped
  def cleanup(): Unit = {
    IOUtils.closeQuietly(featureWriter)
    featureWriter = null
    dataStore = null
    getLogger.info("Shut down geomesa processor")
  }

  override def getRelationships = relationships
  override def getSupportedPropertyDescriptors = descriptors

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
    Option(session.get()).foreach(doWork(context, session, _))

  private def doWork(context: ProcessContext, session: ProcessSession, flowFile: FlowFile): Unit = {
    try {
      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          val ec = converter.createEvaluationContext(Map("inputFilePath" ->
            (flowFile.getAttribute("path") + flowFile.getAttribute("filename"))))
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

  private def getDataStore(context: ProcessContext): DataStore = DataStoreFinder.getDataStore(Map(
    "zookeepers" -> context.getProperty(Zookeepers).getValue,
    "instanceId" -> context.getProperty(InstanceName).getValue,
    "tableName"  -> context.getProperty(Catalog).getValue,
    "user"       -> context.getProperty(User).getValue,
    "password"   -> context.getProperty(Password).getValue
  ))

  private def getSft(context: ProcessContext): SimpleFeatureType = {
    val sftArg = Option(context.getProperty(SftName).getValue)
      .orElse(Option(context.getProperty(SftSpec).getValue))
      .getOrElse(throw new IllegalArgumentException("could not parse spec config"))
        context.getProperty(SftName).getValue
    val typeName = context.getProperty(FeatureName).getValue
    SftArgResolver.getSft(sftArg, typeName).getOrElse(throw new IllegalArgumentException("could not parse sft config"))
  }

  private def createFeatureWriter(sft: SimpleFeatureType, context: ProcessContext): SFW = {
    dataStore.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
  }

  private def getConverter(sft: SimpleFeatureType, context: ProcessContext): convert.SimpleFeatureConverter[_] = {
    val convertArg = Option(context.getProperty(ConverterName).getValue)
      .orElse(Option(context.getProperty(ConverterSpec).getValue))
      .getOrElse(throw new IllegalArgumentException("could not parse converter config"))
    val config = ConverterConfigResolver.getConfig(convertArg).get
    SimpleFeatureConverters.build(sft, config)
  }

}

object GeoMesaIngestProcessor {
  val Zookeepers = new PropertyDescriptor.Builder()
    .name("Zookeepers")
    .description("Zookeepers host(:port) pairs, comma separated")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val InstanceName = new PropertyDescriptor.Builder()
    .name("Instance")
    .description("Accumulo instance name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val User = new PropertyDescriptor.Builder()
    .name("User")
    .description("Accumulo user name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val Password = new PropertyDescriptor.Builder()
    .name("Password")
    .description("Accumulo password")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .sensitive(true)
    .build

  val Catalog = new PropertyDescriptor.Builder()
    .name("Catalog")
    .description("GeoMesa catalog table name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val SftName = new PropertyDescriptor.Builder()
    .name("SftName")
    .description("Choose an SFT defined by a GeoMesa SFT Provider (preferred)")
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

  val FeatureName = new PropertyDescriptor.Builder()
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

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
}