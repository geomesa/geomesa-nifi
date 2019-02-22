package org.geomesa.nifi.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.OnDisabled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.{ProcessContext, ProcessorInitializationContext}
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor
import org.geomesa.nifi.hbase.PutGeoMesaHBase._
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.hbase.data.{HBaseConnectionPool, HBaseDataStoreFactory, HBaseDataStoreParams}

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

    val props = HBaseNifiProps.flatMap { p =>
      val value = context.getProperty(p.getName).getValue
      if (value == null) { Seq.empty } else {
        Seq(p.getName -> value)
      }
    } :+ (HBaseDataStoreParams.ConnectionParam.key -> connection)
    getLogger.trace(s"DataStore Properties: $props")
    DataStoreFinder.getDataStore(props.toMap.asJava)
  }

  def getConnection(context: ProcessContext): Connection = {
    val confFiles = context.getProperty(HBaseConfigFilesProp).getValue.split(',').map(_.trim).map(new Path(_))
    val hbaseConf = HBaseConfiguration.create()
    confFiles.foreach(hbaseConf.addResource)

    HBaseConnectionPool.configureSecurity(hbaseConf)
    HBaseAdmin.checkHBaseAvailable(hbaseConf)
    ConnectionFactory.createConnection(hbaseConf)
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

  val HBaseConfigFilesProp: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("HBaseConfigFiles")
    .description("List of comma separated HBase config files (e.g. hbase-site.xml)")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build()

  val HBaseNifiProps: List[PropertyDescriptor] =
    HBaseDataStoreFactory.ParameterInfo.toList.map(AbstractGeoIngestProcessor.property) :+ HBaseConfigFilesProp
}
