package org.geomesa.nifi.geo

import java.util

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.{DataStore, DataStoreFinder}
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.geomesa.nifi.geo.PutGeoMesaKafka._
import org.locationtech.geomesa.kafka09.{KafkaDataStoreFactoryParams => KDSP}
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "kafka", "streaming", "stream", "geo", "ingest", "convert", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
class PutGeoMesaKafka extends AbstractGeoIngestProcessor {

  protected override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)

    descriptors = (getPropertyDescriptors ++ KdsNifiProps).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  // Abstract
  override protected def getDataStore(context: ProcessContext): DataStore = {
    DataStoreFinder.getDataStore(KdsNifiProps.map { p =>
      p.getName -> context.getProperty(p.getName).getValue
    }.filter(_._2 != null).map { case (p, v) =>
      getLogger.trace(s"ds prop: $p => $v")
      p -> {
        KdsGTProps.find(_.getName == p).head.getType match {
          case x if x.isAssignableFrom(classOf[java.lang.Integer]) => v.toInt
          case x if x.isAssignableFrom(classOf[java.lang.Long])    => v.toLong
          case x if x.isAssignableFrom(classOf[java.lang.Boolean]) => v.toBoolean
          case _                                                   => v
        }
      }
    }.toMap.asJava)
  }

  override protected def getSft(context: ProcessContext): SimpleFeatureType = {
    val sft = super.getSft(context)

    getLogger.info(s"Creating live sft for type ${sft.getTypeName}")
    val zkPath = context.getProperty(KDSP.ZK_PATH.getName).toString
    KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {

    val validationFailures = new util.ArrayList[ValidationResult]()

    // If using converters checkf or params relevant to that
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

}

object PutGeoMesaKafka {
  val KdsGTProps = List(
    KDSP.KAFKA_BROKER_PARAM,
    KDSP.ZOOKEEPERS_PARAM,
    KDSP.ZK_PATH,
    KDSP.NAMESPACE_PARAM,
    KDSP.TOPIC_PARTITIONS,
    KDSP.TOPIC_REPLICATION,
    KDSP.IS_PRODUCER_PARAM,
    KDSP.EXPIRATION_PERIOD,
    KDSP.CLEANUP_LIVE_CACHE
  )

  val KdsNifiProps = KdsGTProps.map { p =>
    new PropertyDescriptor.Builder()
      .name(p.getName)
      .description(p.getDescription.toString)
      .required(p.isRequired)
      .defaultValue(if (p.getDefaultValue != null) p.getDefaultValue.toString else null)
      .addValidator(p.getType match {
        case x if x.isAssignableFrom(classOf[java.lang.Integer]) => StandardValidators.INTEGER_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.Long])    => StandardValidators.LONG_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.Boolean]) => StandardValidators.BOOLEAN_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.String])  => StandardValidators.NON_EMPTY_VALIDATOR
        case _                                                   => StandardValidators.NON_EMPTY_VALIDATOR
      })
      .build()
  }

}
