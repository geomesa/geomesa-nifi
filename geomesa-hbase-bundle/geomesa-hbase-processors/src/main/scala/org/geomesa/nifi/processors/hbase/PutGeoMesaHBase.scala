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
import org.geomesa.nifi.datastore.processor.AwsGeoIngestProcessor
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.hbase.data.{HBaseDataStoreFactory, HBaseDataStoreParams}

@Tags(Array("geomesa", "geo", "ingest", "convert", "hbase", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa HBase")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaHBase
    extends AwsGeoIngestProcessor(PutGeoMesaHBase.HBaseProperties, HBaseDataStoreParams.ConfigsParam)

object PutGeoMesaHBase extends PropertyDescriptorUtils {
  private val HBaseProperties = HBaseDataStoreFactory.ParameterInfo.toList.map(createPropertyDescriptor)
}
