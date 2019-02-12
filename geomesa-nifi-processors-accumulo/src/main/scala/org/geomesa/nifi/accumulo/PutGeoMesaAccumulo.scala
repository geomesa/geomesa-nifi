/***********************************************************************
 * Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.accumulo

import java.util

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor._
import org.geomesa.nifi.accumulo.PutGeoMesaAccumulo._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreFactory, AccumuloDataStoreParams => ADSP}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "convert", "accumulo", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaAccumulo extends AbstractGeoIngestProcessor {

  protected override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)
    descriptors = (getPropertyDescriptors ++ AdsNifiProps).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  /**
    * Flag to be set in validation
    */
  @volatile
  protected var useControllerService: Boolean = false

  protected def getGeomesaControllerService(context: ProcessContext): GeomesaConfigService = {
    context.getProperty(GeoMesaConfigController).asControllerService().asInstanceOf[GeomesaConfigService]
  }

  protected def getDataStoreFromParams(context: ProcessContext): DataStore = {
    val props = AdsNifiProps.flatMap { p =>
      val value = context.getProperty(p.getName).getValue
      if (value == null) { Seq.empty } else {
        Seq(p.getName -> value)
      }
    }
    getLogger.trace(s"DataStore Properties: $props")
    DataStoreFinder.getDataStore(props.toMap.asJava)
  }

  // Abstract
  override protected def getDataStore(context: ProcessContext): DataStore = {
    if (useControllerService) {
      getGeomesaControllerService(context).getDataStore
    } else {
      getDataStoreFromParams(context)
    }
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {

    val validationFailures = new util.ArrayList[ValidationResult]()

    validationFailures.addAll(super.customValidate(validationContext))

    useControllerService = validationContext.getProperty(GeoMesaConfigController).isSet
    val minimumParams = Seq(
      ADSP.InstanceIdParam,
      ADSP.ZookeepersParam,
      ADSP.UserParam,
      ADSP.CatalogParam).map(_.getName)
    val paramsSet = AdsNifiProps.filter(minimumParams contains _.getName).forall(validationContext.getProperty(_).isSet)

    // require either controller-service or all of {zoo,instance,user,catalog}
    if (!useControllerService && !paramsSet)
      validationFailures.add(new ValidationResult.Builder()
        .input("Use either GeoMesa Configuration Service, or specify accumulo connection parameters.")
        .build)

    // Require precisely one of password/keytabPath
    val securityParams = Seq(
      ADSP.PasswordParam,
      ADSP.KeytabPathParam).map(_.getName)
    val numSecurityParams = AdsNifiProps.filter(securityParams contains _.getName).count(validationContext.getProperty(_).isSet)
    if (!useControllerService && numSecurityParams != 1)
      validationFailures.add(new ValidationResult.Builder()
        .input("Precisely one of password and keytabPath must be set.")
        .build)

    validationFailures
  }
}

object PutGeoMesaAccumulo {

  val AdsProps: List[Param] = AccumuloDataStoreFactory.ParameterInfo.toList :+ ADSP.MockParam

  val GeoMesaConfigController: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("GeoMesa Configuration Service")
    .description("The controller service used to connect to Accumulo")
    .required(false)
    .identifiesControllerService(classOf[GeomesaConfigService])
    .build

  // Don't require any properties because we are using the controller service...
  val AdsNifiProps: List[PropertyDescriptor] =
    AdsProps.map(AbstractGeoIngestProcessor.property(_, canBeRequired = false)) :+ GeoMesaConfigController

}
