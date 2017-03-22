/***********************************************************************
* Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.geomesa.nifi.geo

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.DataStoreFinder
import org.geomesa.nifi.geo.PutGeoTools._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "geotools", "datastore", "features", "simple feature"))
@CapabilityDescription("store avro files into geomesa")
class PutGeoTools extends AbstractGeoIngestProcessor {

  //
  // Allow dynamic properties for datastores
  //
  override def getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String): PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .description("Sets the value on the DataStore")
      .name(propertyDescriptorName)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .sensitive(sensitiveProps().contains(propertyDescriptorName))
      .dynamic(true)
      .expressionLanguageSupported(false)
      .build()

  def sensitiveProps() = listDataStores().map(_.getParametersInfo.filter(_.isPassword).map(_.getName)).flatten

  override protected def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)
    this.descriptors = (getPropertyDescriptors ++ List(DataStoreName)).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override protected def getDataStore(context: ProcessContext) = {
    val dsProps = context.getProperties.filter(_._1.getName != DataStoreName.getName).map { case (a, b) => a.getName -> b }
    getLogger.info(s"Looking for DataSore with props $dsProps")
    DataStoreFinder.getDataStore(dsProps)
  }

  //
  // Custom validate properties based on the specific datastore
  //
  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {
    val validationResults = scala.collection.mutable.ListBuffer.empty[ValidationResult]
    val dsOpt = Option(validationContext.getProperty(DataStoreName).getValue)

    if (dsOpt.isDefined && dsOpt.get.nonEmpty) {
      val dsName = dsOpt.get
      getLogger.debug(s"Attempting to validate params for DataSore $dsName")
      val dsParams = listDataStores().filter(_.getDisplayName == dsName).toSeq.head.getParametersInfo
      val required = dsParams.filter(_.isRequired)
      getLogger.debug(s"Required props for DataSore $dsName are ${required.mkString(", ")}")

      val props = validationContext.getProperties.filterKeys(_ != DataStoreName.getName).map { case (a, b) => a.getName -> b }
      val propNames = props.keys

      val missing = required.map(_.getName).toList.filterNot(propNames.contains(_))
      missing.foreach { mp =>
        validationResults +=
          new ValidationResult.Builder()
            .input(mp)
            .valid(false)
            .explanation(s"Required property $mp for DataSore $dsName is missing")
            .build()
      }
    } else {
      validationResults +=
        new ValidationResult.Builder()
          .input(DataStoreName.getName)
          .valid(false)
          .explanation(s"Must define available DataSore name first")
          .build()
    }
    validationResults.asJavaCollection
  }

}

object PutGeoTools {

  private def listDataStores() = DataStoreFinder.getAvailableDataStores

  //
  // Define Properties
  //
  val DataStoreName = new PropertyDescriptor.Builder()
    .name("DataStoreName")
    .description("DataStoreName")
    .allowableValues(listDataStores().map(_.getDisplayName).toArray: _*)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

}

