/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
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
import org.geomesa.nifi.datastore.processor.DataStoreProcessor
import org.geomesa.nifi.datastore.processor.geotools.GeoToolsProcessor.listDataStores
import org.geotools.data.{DataStoreFactorySpi, DataStoreFinder}

abstract class GeoToolsProcessor extends DataStoreProcessor(Seq.empty) {

  import scala.collection.JavaConverters._

  private var dataStoreName: PropertyDescriptor = _

  override protected def init(context: ProcessorInitializationContext): Unit = {
    dataStoreName = GeoToolsProcessor.dataStoreName(listDataStores().map(_.getDisplayName).toSeq)
    super.init(context)
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
        .description("Sets the value on the DataStore")
        .sensitive(GeoToolsProcessor.sensitiveProps().contains(propertyDescriptorName))
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .dynamic(true)
        .build()
  }

  override protected def getConfigProperties: Seq[PropertyDescriptor] =
    Seq(dataStoreName) ++ super.getConfigProperties

  override protected def getDataStoreParams(context: ProcessContext): Map[String, _] = {
    val dynamic = context.getProperties.asScala.collect {
      case (a, b) if a.getName != dataStoreName.getName => a.getName -> b
    }
    super.getDataStoreParams(context) ++ dynamic
  }

  // custom validate properties based on the specific datastore
  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {
    val result = new java.util.ArrayList[ValidationResult]()
    result.addAll(super.customValidate(validationContext))

    val dsName = validationContext.getProperty(dataStoreName).getValue

    def invalid(name: String, reason: String): ValidationResult =
      new ValidationResult.Builder().input(name).valid(false).explanation(reason).build()

    if (dsName == null || dsName.isEmpty) {
      result.add(invalid(dataStoreName.getName, "Must define available DataSore name"))
    } else {
      logger.debug(s"Attempting to validate params for DataSore $dsName")
      val dsParams = GeoToolsProcessor.listDataStores().find(_.getDisplayName == dsName).toSeq.flatMap(_.getParametersInfo)
      val required = dsParams.filter(_.isRequired)
      logger.debug(s"Required props for DataSore $dsName are ${required.mkString(", ")}")

      val names = validationContext.getProperties.asScala.map(_._1.getName).toSet

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

  private def listDataStores(): Iterator[DataStoreFactorySpi] = DataStoreFinder.getAvailableDataStores.asScala

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

