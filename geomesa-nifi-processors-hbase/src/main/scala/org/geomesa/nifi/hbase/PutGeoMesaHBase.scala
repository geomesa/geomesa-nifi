package org.geomesa.nifi.hbase

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor
import org.locationtech.geomesa.hbase.data.{HBaseDataStoreFactory, HBaseDataStoreParams}

@Tags(Array("geomesa", "geo", "ingest", "convert", "hbase", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa HBase")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaHBase extends AbstractGeoIngestProcessor(PutGeoMesaHBase.HBaseProperties)

object PutGeoMesaHBase {
  private val HBaseProperties = HBaseDataStoreFactory.ParameterInfo.toList.map {
    case HBaseDataStoreParams.ConfigPathsParam =>
      // make config paths required - as we don't want to modify the nar to add hbase-site.xml
      val base = AbstractGeoIngestProcessor.property(HBaseDataStoreParams.ConfigPathsParam)
      new PropertyDescriptor.Builder().fromPropertyDescriptor(base).required(true).build()

    case param => AbstractGeoIngestProcessor.property(param)
  }
}
