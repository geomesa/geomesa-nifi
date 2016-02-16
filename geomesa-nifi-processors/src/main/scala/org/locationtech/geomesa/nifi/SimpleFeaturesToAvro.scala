package org.locationtech.geomesa.nifi

import java.io.{InputStream, OutputStream}

import org.apache.nifi.annotation.behavior.WritesAttribute
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.locationtech.geomesa.convert
import org.locationtech.geomesa.convert.{ConverterConfigLoader, ConverterConfigResolver, SimpleFeatureConverters}
import org.locationtech.geomesa.features.avro.AvroDataFileWriter
import org.locationtech.geomesa.nifi.SimpleFeaturesToAvro._
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SimpleFeatureTypeLoader}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

@Tags(Array("OGC", "geo", "convert", "converter", "simple feature", "geotools", "geomesa"))
@CapabilityDescription("Convert incoming files into OGC SimpleFeature avro data files using GeoMesa Converters")
@WritesAttribute(attribute = "mime.type", description = "the mime type of the outgoing format")
class SimpleFeaturesToAvro extends AbstractProcessor {

  private var descriptors: java.util.List[PropertyDescriptor] = null
  private var relationships: java.util.Set[Relationship] = null

  protected override def init(context: ProcessorInitializationContext): Unit = {
    descriptors = List(
      SftName,
      ConverterName,
      FeatureName,
      SftSpec,
      ConverterSpec,
      OutputFormat).asJava
    relationships = Set(SuccessRelationship, FailureRelationship).asJava
  }

  override def getRelationships = relationships
  override def getSupportedPropertyDescriptors = descriptors

  @volatile
  private var converter: convert.SimpleFeatureConverter[_] = null

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    val sft = getSft(context)
    converter = getConverter(sft, context)

    getLogger.info(s"Initialized SimpleFeatureConverter sft and converter for type ${sft.getTypeName}")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
    Option(session.get()).foreach(doWork(context, session, _))

  private def doWork(context: ProcessContext, session: ProcessSession, flowFile: FlowFile): Unit = {
    try {
      val newFlowFile = session.write(flowFile, new StreamCallback {
        override def process(in: InputStream, out: OutputStream): Unit = {
          val dfw = new AvroDataFileWriter(out, converter.targetSFT)
          try {
            val fullFlowFileName = flowFile.getAttribute("path") + flowFile.getAttribute("filename")
            getLogger.info(s"Converting path $fullFlowFileName")
            val ec = converter.createEvaluationContext(Map("inputFilePath" -> fullFlowFileName))
            converter.process(in, ec).foreach(dfw.append)
          } finally {
            dfw.close()
          }
        }
      })
      session.transfer(newFlowFile, SuccessRelationship)
    } catch {
      case e: Exception =>
        getLogger.error(s"Error converter file to avro: ${e.getMessage}", e)
        session.transfer(flowFile, FailureRelationship)
    }
  }

  private def getSft(context: ProcessContext): SimpleFeatureType = {
    val sftArg = Option(context.getProperty(SftName).getValue)
      .orElse(Option(context.getProperty(SftSpec).getValue))
      .getOrElse(throw new IllegalArgumentException("could not parse spec config"))
    context.getProperty(SftName).getValue
    val typeName = context.getProperty(FeatureName).getValue
    SftArgResolver.getSft(sftArg, typeName).getOrElse(throw new IllegalArgumentException("could not parse sft config"))
  }

  private def getConverter(sft: SimpleFeatureType, context: ProcessContext): convert.SimpleFeatureConverter[_] = {
    val convertArg = Option(context.getProperty(ConverterName).getValue)
      .orElse(Option(context.getProperty(ConverterSpec).getValue))
      .getOrElse(throw new IllegalArgumentException("could not parse converter config"))
    val config = ConverterConfigResolver.getConfig(convertArg).get
    SimpleFeatureConverters.build(sft, config)
  }
}

object SimpleFeaturesToAvro {

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

  val OutputFormat = new PropertyDescriptor.Builder()
    .name("OutputFormat")
    .description("File format for the outgoing simple feature file")
    .required(true)
    .allowableValues(Set("avro").asJava)
    .defaultValue("avro")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
}

