package org.geomesa.nifi.redis

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.{ProcessContext, ProcessorInitializationContext}
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.redis.data.RedisDataStoreFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "convert", "redis", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa Redis")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaRedis extends AbstractGeoIngestProcessor {

  import PutGeoMesaRedis.RedisNifiProps

  protected override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)
    descriptors = (getPropertyDescriptors ++ RedisNifiProps).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override protected def getDataStore(context: ProcessContext): DataStore = {
    val props = RedisNifiProps.flatMap { p =>
      val value = context.getProperty(p.getName).getValue
      if (value == null) { Seq.empty } else {
        Seq(p.getName -> value)
      }
    }
    getLogger.trace(s"DataStore Properties: $props")
    DataStoreFinder.getDataStore(props.toMap.asJava)
  }
}

object PutGeoMesaRedis {
  val RedisDsProps: List[Param] = RedisDataStoreFactory.ParameterInfo.toList
  val RedisNifiProps: List[PropertyDescriptor] = RedisDsProps.map(AbstractGeoIngestProcessor.property)
}
