/***********************************************************************
 * Copyright (c) 2015-2019 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors
package datastore

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor.ProcessContext
import org.geomesa.nifi.controller.api.DataStoreConfigService
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreFactory, AccumuloDataStoreParams}

@Tags(Array("geomesa", "geo", "ingest", "convert", "accumulo", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaAccumulo extends AbstractGeoIngestProcessor(PutGeoMesaAccumulo.AccumuloProperties) {

  import AccumuloDataStoreParams._
  import PutGeoMesaAccumulo._

  import scala.collection.JavaConverters._

  /**
    * Flag to be set in validation
    */
  @volatile
  private var useControllerService: Boolean = false

  override protected def getDataStoreParams(context: ProcessContext): Map[String, _] = {
    if (useControllerService) {
      val controller = context.getProperty(GeoMesaConfigController)
      controller.asControllerService(classOf[DataStoreConfigService]).getDataStoreParameters.asScala.toMap
    } else {
      super.getDataStoreParams(context)
    }
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {
    import AbstractGeoIngestProcessor.invalid

    val result = new java.util.ArrayList[ValidationResult]()
    result.addAll(super.customValidate(validationContext))

    useControllerService = validationContext.getProperty(GeoMesaConfigController).isSet

    if (!useControllerService) {
      val required = Seq(InstanceIdParam, ZookeepersParam, UserParam, CatalogParam).map(_.getName)
      val missing = AccumuloProperties.exists { p =>
        required.contains(p.getName) && !validationContext.getProperty(p).isSet
      }

      // require either controller-service or all of {zoo,instance,user,catalog}
      if (missing) {
        result.add(invalid("Use either GeoMesa Configuration Service, or specify accumulo connection parameters"))
      }

      // Require precisely one of password/keytabPath
      val authentication = Seq(PasswordParam, KeytabPathParam).map(_.getName)
      val numSecurityParams = AccumuloProperties.count { p =>
        authentication.contains(p.getName) && validationContext.getProperty(p).isSet
      }
      if (numSecurityParams != 1) {
        result.add(invalid("Precisely one of password and keytabPath must be set"))
      }
    }

    result
  }
}

object PutGeoMesaAccumulo {

  val GeoMesaConfigController: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("Data Store Configuration Service")
        .required(false)
        .description("The controller service used to connect to Accumulo")
        .identifiesControllerService(classOf[DataStoreConfigService])
        .build()

  private val AccumuloProperties = {
    val params = AccumuloDataStoreFactory.ParameterInfo.toList :+ AccumuloDataStoreParams.MockParam
    // don't require any properties because we are using the controller service
    params.map(p => unrequired(createPropertyDescriptor(p))) :+ GeoMesaConfigController
  }
}
