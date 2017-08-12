package org.geomesa.nifi.hbase

import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.OnDisabled
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.{ProcessContext, ProcessorInitializationContext}
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.geomesa.nifi.geo.{AbstractGeoIngestProcessor, IngestMode}
import org.geomesa.nifi.hbase.PutGeoMesaHBase._
import org.geotools.data.{DataStore, DataStoreFinder, Parameter}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "convert", "hbase", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa HBase")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaHBase extends AbstractGeoIngestProcessor {

  @volatile private[this] var connection: Connection = _

  protected override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)
    descriptors = (getPropertyDescriptors ++ HBaseNifiProps).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override protected def getDataStore(context: ProcessContext): DataStore = {
    connection = getConnection(context)

    val dsProps = Map("connection" -> connection) ++ HBaseNifiProps.map { p =>
      p.getName -> context.getProperty(p.getName).getValue
    }.filter { case (p, v) => v != null && HBDSProps.exists(_.getName == p) }
     .map { case (p, v) =>
       getLogger.trace(s"DataStore Properties: $p => $v")
       p -> {
        HBDSProps.find(_.getName == p).map { opt =>
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

  def getConnection(context: ProcessContext): Connection = {
    val confFiles = context.getProperty(HBaseConfigFilesProp).getValue.split(',').map(_.trim).map(new Path(_))
    val hbaseConf = HBaseConfiguration.create()
    confFiles.foreach(hbaseConf.addResource)

    HBaseDataStoreFactory.configureSecurity(hbaseConf)
    HBaseAdmin.checkHBaseAvailable(hbaseConf)
    ConnectionFactory.createConnection(hbaseConf)
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

    if (connection != null) {
      try {
        connection.close()
      } catch {
        case e: Exception =>
          getLogger.error("Error closing hbase connection", e)
      }
    }
  }

}

object PutGeoMesaHBase {

  import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
  val HBDSProps = List(
    BigTableNameParam,
    CoprocessorUrl,
    EnableSecurityParam
  )

  val HBaseConfigFilesProp: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("HBaseConfigFiles")
    .description("List of comma separated HBase config files (e.g. hbase-site.xml)")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build()

  // Don't require any properties because we are using the controller service...
  val HBaseNifiProps: List[PropertyDescriptor] = HBDSProps.map { p =>
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
  } ++ List(HBaseConfigFilesProp)
}