/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.hbase

import org.geomesa.nifi.datastore.processor.mixins.{AbstractDataStoreProcessor, AwsDataStoreProcessor}
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.hbase.data.{HBaseDataStoreFactory, HBaseDataStoreParams}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

abstract class HBaseProcessor
    extends AbstractDataStoreProcessor(HBaseProcessor.HBaseProperties) with AwsDataStoreProcessor {
  override protected def configParam: GeoMesaParam[String] = HBaseDataStoreParams.ConfigsParam
}

object HBaseProcessor extends PropertyDescriptorUtils {
  private val HBaseProperties = createPropertyDescriptors(HBaseDataStoreFactory)
}
