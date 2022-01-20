/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.hbase

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.geomesa.nifi.datastore.processor.service.AwsDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.hbase.data.{HBaseDataStoreFactory, HBaseDataStoreParams}

@Tags(Array("geomesa", "geotools", "geo", "hbase"))
@CapabilityDescription("Service for connecting to GeoMesa HBase stores")
class HBaseDataStoreService
    extends AwsDataStoreService[HBaseDataStoreFactory](HBaseDataStoreService.Properties, HBaseDataStoreParams.ConfigsParam)

object HBaseDataStoreService extends PropertyDescriptorUtils {
  val Properties: Seq[PropertyDescriptor] = createPropertyDescriptors(HBaseDataStoreFactory)
}
