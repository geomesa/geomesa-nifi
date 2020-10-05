/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import java.io.{ByteArrayInputStream, StringWriter}
import java.nio.charset.StandardCharsets

import com.amazonaws.auth.AWSSessionCredentials
import org.apache.hadoop.conf.Configuration
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

/**
  * Trait with support for an AWSCredentialsProviderService
  */
trait AwsGeoIngestProcessor extends AbstractDataStoreProcessor {

  import scala.collection.JavaConverters._

  /**
   * Parameter for embedding hadoop &lt;configuration&gt; xml, used to pass the AWS credentials
   *
   * @return
   */
  protected def configParam: GeoMesaParam[String]

  override protected def getServiceProperties: Seq[PropertyDescriptor] =
    super.getServiceProperties ++ Seq(AwsGeoIngestProcessor.CredentialsServiceProperty)

  override protected def getDataStoreParams(context: ProcessContext): Map[String, _] = {
    val base = super.getDataStoreParams(context)
    val prop = context.getProperty(AwsGeoIngestProcessor.CredentialsServiceProperty)
    val credentials = for {
      service  <- Option(prop.asControllerService(classOf[AWSCredentialsProviderService]))
      provider <- Option(service.getCredentialsProvider)
    } yield {
      provider.getCredentials
    }
    credentials match {
      case None => base
      case Some(c) =>
        val config = new Configuration(false)
        config.set("fs.s3a.access.key", c.getAWSAccessKeyId)
        config.set("fs.s3a.secret.key", c.getAWSSecretKey)
        c match {
          case s: AWSSessionCredentials =>
            config.set("fs.s3a.session.token", s.getSessionToken)
            // TODO handle session renewal?
            config.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
          case _ => // no-op
        }
        // add any config that was populated by the user so it's not lost
        configParam.lookupOpt(base.asJava).foreach { c =>
          config.addResource(new ByteArrayInputStream(c.getBytes(StandardCharsets.UTF_8)))
        }
        val out = new StringWriter()
        config.writeXml(out)
        base ++ Map(configParam.key -> out.toString)
    }
  }
}

object AwsGeoIngestProcessor {
  private val CredentialsServiceProperty =
    new PropertyDescriptor.Builder()
        .name("AWS Credentials Provider service")
        .description("The Controller Service that is used to obtain an AWS credentials provider")
        .required(false)
        .identifiesControllerService(classOf[AWSCredentialsProviderService])
        .build()
}
