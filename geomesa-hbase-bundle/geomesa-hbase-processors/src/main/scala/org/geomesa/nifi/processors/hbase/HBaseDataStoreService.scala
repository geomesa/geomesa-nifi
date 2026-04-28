/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.geomesa.nifi.datastore.processor.service.AwsDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.hbase.data.{HBaseDataStoreFactory, HBaseDataStoreParams}
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials

import java.io.{ByteArrayInputStream, StringWriter}
import java.nio.charset.StandardCharsets

@Tags(Array("geomesa", "geotools", "geo", "hbase"))
@CapabilityDescription("Service for connecting to GeoMesa HBase stores")
class HBaseDataStoreService extends AwsDataStoreService[HBaseDataStoreFactory](HBaseDataStoreService.Properties) {

  import scala.collection.JavaConverters._

  override protected def getDataStoreParams(context: PropertyContext): Map[String, _ <: AnyRef] = {
    val base = super.getDataStoreParams(context)
    getCredentials(context) match {
      case None => base
      case Some(c) =>
        val config = new Configuration(false)
        config.set("fs.s3a.access.key", c.accessKeyId())
        config.set("fs.s3a.secret.key", c.secretAccessKey())
        c match {
          case s: AwsSessionCredentials =>
            config.set("fs.s3a.session.token", s.sessionToken())
            // TODO handle session renewal?
            config.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
          case _ => // no-op
        }
        // add any config that was populated by the user so it's not lost
        HBaseDataStoreParams.ConfigsParam.lookupOpt(base.asJava).foreach { c =>
          config.addResource(new ByteArrayInputStream(c.getBytes(StandardCharsets.UTF_8)))
        }
        val out = new StringWriter()
        config.writeXml(out)
        base ++ Map(HBaseDataStoreParams.ConfigsParam.key -> out.toString)
    }
  }
}

object HBaseDataStoreService extends PropertyDescriptorUtils {
  val Properties: Seq[PropertyDescriptor] = createPropertyDescriptors(HBaseDataStoreFactory)
}
