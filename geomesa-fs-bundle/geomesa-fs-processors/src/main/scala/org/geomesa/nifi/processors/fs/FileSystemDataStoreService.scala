/***********************************************************************
 * Copyright (c) 2015-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.fs

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.geomesa.nifi.datastore.processor.service.AwsDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams

@Tags(Array("geomesa", "geotools", "geo", "hdfs", "s3"))
@CapabilityDescription("Service for connecting to GeoMesa FileSystem stores")
class FileSystemDataStoreService
    extends AwsDataStoreService[FileSystemDataStoreFactory](FileSystemDataStoreService.Properties, FileSystemDataStoreParams.ConfigsParam)

object FileSystemDataStoreService extends PropertyDescriptorUtils {
  val Properties: Seq[PropertyDescriptor] = createPropertyDescriptors(FileSystemDataStoreFactory)
}
