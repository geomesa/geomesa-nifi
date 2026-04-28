/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.fs

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.geomesa.nifi.datastore.processor.service.AwsDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.fs.data.{FileSystemDataStoreFactory, FileSystemDataStoreParams}

import java.io.StringWriter
import java.util.Properties

@Tags(Array("geomesa", "geotools", "geo", "hdfs", "s3"))
@CapabilityDescription("Service for connecting to GeoMesa FileSystem stores")
class FileSystemDataStoreService extends AwsDataStoreService[FileSystemDataStoreFactory](FileSystemDataStoreService.Properties) {

  import scala.collection.JavaConverters._

  override protected def getDataStoreParams(context: PropertyContext): Map[String, _ <: AnyRef] = {
    val base = super.getDataStoreParams(context)
    getCredentials(context) match {
      case None => base
      case Some(c) =>
        val props = FileSystemDataStoreParams.ConfigParam.lookupOpt(base.asJava).getOrElse(new Properties())
        props.put("fs.s3.access-key-id", c.accessKeyId())
        props.put("fs.s3.secret-access-key", c.secretAccessKey())
        val out = new StringWriter()
        props.store(out, null)
        base ++ Map(FileSystemDataStoreParams.ConfigParam.key -> out.toString)
    }
  }
}

object FileSystemDataStoreService extends PropertyDescriptorUtils {
  val Properties: Seq[PropertyDescriptor] = createPropertyDescriptors(FileSystemDataStoreFactory)
}
