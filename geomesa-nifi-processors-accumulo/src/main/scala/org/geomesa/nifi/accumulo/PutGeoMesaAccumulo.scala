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
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.accumulo.PutGeoMesaAccumulo._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.geomesa.nifi.geo.{AbstractGeoIngestProcessor, IngestMode}
import org.geotools.data.{DataStore, DataStoreFinder, Parameter}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreParams => ADSP}

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

  protected def getDataStoreFromParams(context: ProcessContext): DataStore =
    DataStoreFinder.getDataStore(AdsNifiProps.map { p =>
      p.getName -> context.getProperty(p.getName).getValue
    }.filter(_._2 != null).map { case (p, v) =>
      getLogger.trace(s"DataStore Properties: $p => $v")
      p -> {
        AdsProps.find(_.getName == p).head.getType match {
          case x if x.isAssignableFrom(classOf[java.lang.Integer]) => v.toInt
          case x if x.isAssignableFrom(classOf[java.lang.Long])    => v.toLong
          case x if x.isAssignableFrom(classOf[java.lang.Boolean]) => v.toBoolean
          case _                                                   => v
        }
      }
    }.toMap.asJava)

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

    useControllerService = validationContext.getProperty(GeoMesaConfigController).isSet
    val minimumParams = Seq(
      ADSP.instanceIdParam,
      ADSP.zookeepersParam,
      ADSP.userParam,
      ADSP.tableNameParam).map(_.getName)
    val paramsSet = AdsNifiProps.filter(minimumParams contains _.getName).forall(validationContext.getProperty(_).isSet)

    // require either controller-service or all of {zoo,instance,user,catalog}
    if (!useControllerService && !paramsSet)
      validationFailures.add(new ValidationResult.Builder()
        .input("Use either GeoMesa Configuration Service, or specify accumulo connection parameters.")
        .build)

    // Require precisely one of password/keytabPath
    val securityParams = Seq(
      ADSP.passwordParam,
      ADSP.keytabPathParam).map(_.getName)
    val numSecurityParams = AdsNifiProps.filter(securityParams contains _.getName).count(validationContext.getProperty(_).isSet)
    if (!useControllerService && numSecurityParams != 1)
      validationFailures.add(new ValidationResult.Builder()
        .input("Precisely one of password and keytabPath must be set.")
        .build)

    // If using converters check for params relevant to that
    def useConverter = validationContext.getProperty(IngestModeProp).getValue == IngestMode.Converter
    if (useConverter) {
      // make sure either a sft is named or written
      val sftNameSet = validationContext.getProperty(SftName).isSet
      val sftSpecSet = validationContext.getProperty(SftSpec).isSet
      if (!sftNameSet && !sftSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a simple feature type by name or spec")
          .build)

      val convNameSet = validationContext.getProperty(ConverterName).isSet
      val convSpecSet = validationContext.getProperty(ConverterSpec).isSet
      if (!convNameSet && !convSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a converter by name or spec")
          .build
        )
    }

    validationFailures
  }

}

object PutGeoMesaAccumulo {

  val AdsProps = List(
    ADSP.instanceIdParam,
    ADSP.zookeepersParam,
    ADSP.userParam,
    ADSP.passwordParam,
    ADSP.keytabPathParam,
    ADSP.visibilityParam,
    ADSP.tableNameParam,
    ADSP.writeThreadsParam,
    ADSP.generateStatsParam,
    ADSP.mockParam
  )

  val GeoMesaConfigController = new PropertyDescriptor.Builder()
    .name("GeoMesa Configuration Service")
    .description("The controller service used to connect to Accumulo")
    .required(false)
    .identifiesControllerService(classOf[GeomesaConfigService])
    .build

  // Don't require any properties because we are using the controller service...
  val AdsNifiProps = AdsProps.map { p =>
    new PropertyDescriptor.Builder()
      .name(p.getName)
      .description(p.getDescription.toString)
      .defaultValue(if (p.getDefaultValue != null) p.getDefaultValue.toString else null)
      .addValidator(p.getType match {
        case x if x.isAssignableFrom(classOf[java.lang.Integer]) => StandardValidators.INTEGER_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.Long])    => StandardValidators.LONG_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.Boolean]) => StandardValidators.BOOLEAN_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.String])  => StandardValidators.NON_EMPTY_VALIDATOR
        case _                                                   => StandardValidators.NON_EMPTY_VALIDATOR
      })
      .sensitive(p.metadata.getOrDefault(Parameter.IS_PASSWORD, java.lang.Boolean.FALSE).asInstanceOf[java.lang.Boolean] == java.lang.Boolean.TRUE)
      .build()
  } ++ List(GeoMesaConfigController)

}