/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.fs

import org.geomesa.nifi.datastore.processor.mixins.{AwsDataStoreProcessor, DataStoreProcessor}
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

abstract class FileSystemProcessor
    extends DataStoreProcessor(FileSystemProcessor.FileSystemProperties) with AwsDataStoreProcessor {
  override protected def configParam: GeoMesaParam[String] = FileSystemDataStoreParams.ConfigsParam
}

object FileSystemProcessor extends PropertyDescriptorUtils {
  private [fs] val FileSystemProperties = createPropertyDescriptors(FileSystemDataStoreFactory)
}
