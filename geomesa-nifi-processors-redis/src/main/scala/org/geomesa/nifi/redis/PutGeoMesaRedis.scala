package org.geomesa.nifi.redis

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor
import org.locationtech.geomesa.redis.data.RedisDataStoreFactory

@Tags(Array("geomesa", "geo", "ingest", "convert", "redis", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa Redis")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaRedis extends AbstractGeoIngestProcessor(PutGeoMesaRedis.RedisProperties)

object PutGeoMesaRedis {
  private val RedisProperties = RedisDataStoreFactory.ParameterInfo.toList.map(AbstractGeoIngestProcessor.property)
}
