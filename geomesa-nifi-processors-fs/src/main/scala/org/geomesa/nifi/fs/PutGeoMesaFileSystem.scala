package org.geomesa.nifi.fs

import java.util

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.OnDisabled
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.{ProcessContext, ProcessorInitializationContext}
import org.geomesa.nifi.fs.PutGeoMesaFileSystem._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.geomesa.nifi.geo.{AbstractGeoIngestProcessor, IngestMode}
import org.geotools.data.{DataStore, DataStoreFinder, Parameter}
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.conf.{PartitionSchemeArgResolver, SchemeArgs}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "convert", "hdfs", "s3", "geotools"))
@CapabilityDescription("Convert and ingest data files into a GeoMesa FileSystem Datastore")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaFileSystem extends AbstractGeoIngestProcessor {

  protected override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)
    descriptors = (getPropertyDescriptors ++ FSNifiProps).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override protected def getSft(context: ProcessContext): SimpleFeatureType = {
    val sft = super.getSft(context)

    if (context.getProperty(PartitionSchemeParam).getValue != null) {
      getLogger.info(s"Adding partition scheme to ${sft.getTypeName}")
      val psString = context.getProperty(PartitionSchemeParam).getValue
      val scheme = PartitionSchemeArgResolver.getArg(SchemeArgs(psString, sft)) match {
        case Left(e) => throw new IllegalArgumentException(e)
        case Right(s) => s
      }
      PartitionScheme.addToSft(sft, scheme)
      getLogger.info(s"Updated SFT with partition scheme: ${scheme.getName()}")
    }

    sft
  }

  override protected def getDataStore(context: ProcessContext): DataStore = {
    val dsProps = FSNifiProps.map { p =>
      p.getName -> context.getProperty(p.getName).getValue
    }.filter { case (p, v) => v != null && FSDSProps.exists(_.getName == p) }
     .map { case (p, v) =>
       getLogger.trace(s"DataStore Properties: $p => $v")
       p -> {
         FSDSProps.find(_.getName == p).map { opt =>
          opt.getType match {
            case x if x.isAssignableFrom(classOf[java.lang.Integer]) => v.toInt
            case x if x.isAssignableFrom(classOf[java.lang.Long]) => v.toLong
            case x if x.isAssignableFrom(classOf[java.lang.Boolean]) => v.toBoolean
            case _ => v
          }
        }.getOrElse({throw new IllegalArgumentException(s"Error: unable to determine type of property $p")})
      }
    }.toMap.asJava

    DataStoreFinder.getDataStore(dsProps)
  }


  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {

    val validationFailures = new util.ArrayList[ValidationResult]()

    // If using converters check for params relevant to that
    def useConverter = validationContext.getProperty(IngestModeProp).getValue == IngestMode.Converter
    if (useConverter) {
      // make sure either a sft is named or written
      val sftNameSet = validationContext.getProperty(SftName).isSet
      val sftSpecSet = validationContext.getProperty(SftSpec).isSet
      if (!sftNameSet && !sftSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a simple feature type by name or spec")
          .build)

      val convNameSet = validationContext.getProperty(ConverterName).isSet
      val convSpecSet = validationContext.getProperty(ConverterSpec).isSet
      if (!convNameSet && !convSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a converter by name or spec")
          .build
        )
    }

    validationFailures
  }

  @OnDisabled
  override def cleanup(): Unit = {
    super.cleanup()
  }

}

object PutGeoMesaFileSystem {
  val FSDSProps = List(
    FileSystemDataStoreParams.EncodingParam,
    FileSystemDataStoreParams.PathParam
  )

  val PartitionSchemeParam: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("PartitionScheme")
    .description("A partition scheme common name or config (required for creation of new store)")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build()

  // Don't require any properties because we are using the controller service...
  val FSNifiProps: List[PropertyDescriptor] = FSDSProps.map { p =>
    new PropertyDescriptor.Builder()
      .name(p.getName)
      .description(p.getDescription.toString)
      .defaultValue(if (p.getDefaultValue != null) p.getDefaultValue.toString else null)
      .required(p.required)
      .addValidator(p.getType match {
        case x if x.isAssignableFrom(classOf[java.lang.Integer]) => StandardValidators.INTEGER_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.Long])    => StandardValidators.LONG_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.Boolean]) => StandardValidators.BOOLEAN_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.String])  => StandardValidators.NON_EMPTY_VALIDATOR
        case _                                                   => StandardValidators.NON_EMPTY_VALIDATOR
      })
      .sensitive(p.metadata.getOrDefault(Parameter.IS_PASSWORD, java.lang.Boolean.FALSE).asInstanceOf[java.lang.Boolean] == java.lang.Boolean.TRUE)
      .build()
  } ++ List(PartitionSchemeParam)
}