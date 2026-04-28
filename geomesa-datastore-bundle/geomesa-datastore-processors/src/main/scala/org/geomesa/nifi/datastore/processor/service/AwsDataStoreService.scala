/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.service

import org.apache.hadoop.conf.Configuration
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService
import org.geotools.api.data.DataStoreFactorySpi
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import software.amazon.awssdk.auth.credentials.{AwsCredentials, AwsSessionCredentials}

import java.io.{ByteArrayInputStream, StringWriter}
import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag

/**
 * Aws data store integration
 *
 * @param descriptors data store descriptors
 */
class AwsDataStoreService[T <: DataStoreFactorySpi: ClassTag](descriptors: Seq[PropertyDescriptor])
    extends GeoMesaDataStoreService(descriptors ++ Seq(AwsDataStoreService.Properties.CredentialsServiceProperty)) {
  protected def getCredentials(context: PropertyContext): Option[AwsCredentials] = {
    val prop = context.getProperty(AwsDataStoreService.Properties.CredentialsServiceProperty)
    for {
      service  <- Option(prop.asControllerService(classOf[AwsCredentialsProviderService]))
      provider <- Option(service.getAwsCredentialsProvider)
    } yield {
      provider.resolveCredentials()
    }
  }
}

object AwsDataStoreService {
  object Properties {
    val CredentialsServiceProperty: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("AWS Credentials Provider service")
          .description("The Controller Service that is used to obtain an AWS credentials provider")
          .required(false)
          .identifiesControllerService(classOf[AwsCredentialsProviderService])
          .build()
  }
}
