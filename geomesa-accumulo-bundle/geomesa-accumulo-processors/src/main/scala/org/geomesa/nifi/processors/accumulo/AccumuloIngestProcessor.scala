/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.accumulo

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor.ProcessContext
import org.geomesa.nifi.datastore.processor.AbstractGeoIngestProcessor
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geomesa.nifi.datastore.services.DataStoreConfigService
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreFactory, AccumuloDataStoreParams}

@Tags(Array("geomesa", "geo", "ingest", "convert", "accumulo", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
abstract class AccumuloIngestProcessor extends AbstractGeoIngestProcessor(AccumuloIngestProcessor.AccumuloProperties) {

  import AccumuloDataStoreParams._
  import AccumuloIngestProcessor._

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

object AccumuloIngestProcessor extends PropertyDescriptorUtils {

  val GeoMesaConfigController: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("Data Store Configuration Service")
        .required(false)
        .description("The controller service used to connect to Accumulo")
        .identifiesControllerService(classOf[DataStoreConfigService])
        .build()

  private val AccumuloProperties =
    // don't require any properties because we are using the controller service
    createPropertyDescriptors(AccumuloDataStoreFactory).map(unrequired) :+ GeoMesaConfigController
}
