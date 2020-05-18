/***********************************************************************
 * Copyright (c) 2015-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.datastore

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.processors.AbstractGeoIngestProcessor
import org.geotools.data.{DataStoreFactorySpi, DataStoreFinder}

@Tags(Array("geomesa", "geo", "ingest", "geotools", "datastore", "features", "simple feature"))
@CapabilityDescription("store avro files into geomesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoTools extends AbstractGeoIngestProcessor(Seq(PutGeoTools.DataStoreName)) {

  import PutGeoTools.DataStoreName

  import scala.collection.JavaConverters._

  /**
    * Allow dynamic properties for data stores
    *
    * @param propertyDescriptorName name
    * @return
    */
  override def getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String): PropertyDescriptor = {
    new PropertyDescriptor.Builder()
        .name(propertyDescriptorName)
        .description("Sets the value on the DataStore")
        .sensitive(PutGeoTools.sensitiveProps().contains(propertyDescriptorName))
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .dynamic(true)
        .build()
  }

  override protected def getDataStoreParams(context: ProcessContext): Map[String, _] = {
    val dynamic = context.getProperties.asScala.collect {
      case (a, b) if a.getName != DataStoreName.getName => a.getName -> b
    }
    super.getDataStoreParams(context) ++ dynamic
  }

  // custom validate properties based on the specific datastore
  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {
    val result = new java.util.ArrayList[ValidationResult]()
    result.addAll(super.customValidate(validationContext))

    val dsName = validationContext.getProperty(DataStoreName).getValue

    def invalid(name: String, reason: String): ValidationResult =
      new ValidationResult.Builder().input(name).valid(false).explanation(reason).build()

    if (dsName == null || dsName.isEmpty) {
      result.add(invalid(DataStoreName.getName, "Must define available DataSore name"))
    } else {
      logger.debug(s"Attempting to validate params for DataSore $dsName")
      val dsParams = PutGeoTools.listDataStores().find(_.getDisplayName == dsName).toSeq.flatMap(_.getParametersInfo)
      val required = dsParams.filter(_.isRequired)
      logger.debug(s"Required props for DataSore $dsName are ${required.mkString(", ")}")

      val names = validationContext.getProperties.asScala.map(_._1.getName).toSet

      required.foreach { p =>
        val name = p.getName
        if (names.contains(name)) {
          result.add(invalid(name, s"Required property $name for DataSore $dsName is missing"))
        }
      }
    }

    result
  }
}

object PutGeoTools {

  import scala.collection.JavaConverters._

  private def listDataStores(): Iterator[DataStoreFactorySpi] = DataStoreFinder.getAvailableDataStores.asScala

  private def sensitiveProps(): Iterator[String] =
    listDataStores().flatMap(_.getParametersInfo.collect { case i if i.isPassword => i.getName })

  val DataStoreName: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("DataStoreName")
        .required(true)
        .description("Name of the GeoTools data store to use")
        .allowableValues(listDataStores().map(_.getDisplayName).toArray: _*)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build()
}

