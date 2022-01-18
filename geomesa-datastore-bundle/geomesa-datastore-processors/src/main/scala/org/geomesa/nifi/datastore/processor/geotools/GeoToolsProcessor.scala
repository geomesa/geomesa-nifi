/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.geotools

import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.mixins.AbstractDataStoreProcessor
import org.geotools.data.{DataStoreFactorySpi, DataStoreFinder}

abstract class GeoToolsProcessor extends AbstractDataStoreProcessor(Seq.empty) {

  import scala.collection.JavaConverters._

  private var dataStoreName: PropertyDescriptor = _

  override def reloadDescriptors(): Unit = {
    dataStoreName = GeoToolsProcessor.dataStoreName(GeoToolsProcessor.listDataStores().map(_.getDisplayName).toSeq)
    super.reloadDescriptors()
  }

  /**
    * Allow dynamic properties for data stores
    *
    * @param propertyDescriptorName name
    * @return
    */
  override def getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String): PropertyDescriptor = {
    new PropertyDescriptor.Builder()
        .name(propertyDescriptorName)
        .description("Use this value in the DataStore lookup")
        .sensitive(GeoToolsProcessor.sensitiveProps().contains(propertyDescriptorName))
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .dynamic(true)
        .build()
  }

  override protected def getConfigProperties: Seq[PropertyDescriptor] =
    Seq(dataStoreName) ++ super.getConfigProperties

  override protected def getDataStoreParams(context: ProcessContext): Map[String, _] = {
    val params = context.getProperties.asScala.collect {
      case (a, _) if a.getName != dataStoreName.getName => a.getName ->
          context.getProperty(a).evaluateAttributeExpressions().getValue
    }
    params.toMap
  }

  // custom validate properties based on the specific datastore
  override protected def customValidate(context: ValidationContext): java.util.Collection[ValidationResult] = {
    val result = new java.util.ArrayList[ValidationResult]()
    result.addAll(super.customValidate(context))

    val dsName = context.getProperty(dataStoreName).getValue

    def invalid(name: String, reason: String): ValidationResult =
      new ValidationResult.Builder().input(name).valid(false).explanation(reason).build()

    if (dsName == null || dsName.isEmpty) {
      result.add(invalid(dataStoreName.getName, "Must define available DataSore name"))
    } else {
      logger.debug(s"Attempting to validate params for DataSore $dsName")
      val dsParams = GeoToolsProcessor.listDataStores().find(_.getDisplayName == dsName).toSeq.flatMap(_.getParametersInfo)
      val required = dsParams.filter(_.isRequired)
      logger.debug(s"Required props for DataSore $dsName are ${required.mkString(", ")}")

      val names = context.getProperties.asScala.map(_._1.getName).toSet

      required.foreach { p =>
        val name = p.getName
        if (!names.contains(name)) {
          result.add(invalid(name, s"Required property $name for DataSore $dsName is missing"))
        }
      }
    }

    result
  }
}

object GeoToolsProcessor {

  import scala.collection.JavaConverters._

  private def listDataStores(): Iterator[DataStoreFactorySpi] = {
    DataStoreFinder.scanForPlugins() // ensure stores on the current classpath are loaded
    DataStoreFinder.getAvailableDataStores.asScala
  }

  private def sensitiveProps(): Iterator[String] =
    listDataStores().flatMap(_.getParametersInfo.collect { case i if i.isPassword => i.getName })

  def dataStoreName(values: Seq[String]): PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("DataStoreName")
        .required(true)
        .description("The GeoTools data store type to use")
        .allowableValues(values.sorted: _*)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build()
}

