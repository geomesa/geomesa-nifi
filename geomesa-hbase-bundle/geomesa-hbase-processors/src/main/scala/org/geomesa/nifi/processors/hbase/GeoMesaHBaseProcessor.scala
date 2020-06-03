/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.hbase

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geomesa.nifi.datastore.processor.{AbstractGeoIngestProcessor, AwsGeoIngestProcessor}
import org.locationtech.geomesa.hbase.data.{HBaseDataStoreFactory, HBaseDataStoreParams}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

@Tags(Array("geomesa", "geo", "ingest", "convert", "hbase", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa HBase")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
abstract class GeoMesaHBaseProcessor
    extends AbstractGeoIngestProcessor(GeoMesaHBaseProcessor.HBaseProperties) with AwsGeoIngestProcessor {
  override protected def configParam: GeoMesaParam[String] = HBaseDataStoreParams.ConfigsParam
}

object GeoMesaHBaseProcessor extends PropertyDescriptorUtils {
  private val HBaseProperties = HBaseDataStoreFactory.ParameterInfo.toList.map(createPropertyDescriptor)
}
