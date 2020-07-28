/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.Parameter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreInfo
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import scala.util.control.NonFatal

trait PropertyDescriptorUtils extends LazyLogging {

  /**
   * Create descriptors for a data store
   *
   * @param info parameter info
   * @return
   */
  def createPropertyDescriptors(info: GeoMesaDataStoreInfo): List[PropertyDescriptor] =
    info.ParameterInfo.toList.collect { case p if p.readWrite.append => createPropertyDescriptor(p) }

  /**
   * Creates a nifi property descriptor based on a geotools data store parameter
   *
   * @param param param
   * @return
   */
  def createPropertyDescriptor(param: Param): PropertyDescriptor = {
    val validator = param.getType match {
      case x if classOf[java.lang.Integer].isAssignableFrom(x) => StandardValidators.INTEGER_VALIDATOR
      case x if classOf[java.lang.Long].isAssignableFrom(x)    => StandardValidators.LONG_VALIDATOR
      case x if classOf[java.lang.Boolean].isAssignableFrom(x) => StandardValidators.BOOLEAN_VALIDATOR
      case x if classOf[java.lang.String].isAssignableFrom(x)  => StandardValidators.NON_EMPTY_VALIDATOR
      case _                                                   => StandardValidators.NON_EMPTY_VALIDATOR
    }
    val sensitive =
      Option(param.metadata.get(Parameter.IS_PASSWORD).asInstanceOf[java.lang.Boolean]).exists(_.booleanValue)
    val expression = if (param.metadata.get(GeoMesaParam.SupportsNiFiExpressions) == java.lang.Boolean.TRUE) {
      ExpressionLanguageScope.VARIABLE_REGISTRY
    } else {
      ExpressionLanguageScope.NONE
    }

    val builder =
      new PropertyDescriptor.Builder()
          .name(param.getName)
          .description(param.getDescription.toString)
          .defaultValue(Option(param.getDefaultValue).map(_.toString.trim).filterNot(_.isEmpty).orNull)
          .required(param.required)
          .addValidator(validator)
          .expressionLanguageSupported(expression)
          .sensitive(sensitive)

    if (classOf[java.lang.Boolean].isAssignableFrom(param.getType)) {
      builder.allowableValues("true", "false")
    } else {
      Option(param.metadata.get(Parameter.OPTIONS)).foreach { enum =>
        try { builder.allowableValues(new java.util.HashSet(enum.asInstanceOf[java.util.List[String]])) } catch {
          case NonFatal(e) => logger.warn(s"Error trying to set allowable values for ${param.getName}: $enum", e)
        }
      }
    }

    builder.build()
  }

  /**
   * Create a new property descriptor that has required == false
   *
   * @param prop base property descriptor
   * @return
   */
  def unrequired(prop: PropertyDescriptor): PropertyDescriptor =
    new PropertyDescriptor.Builder().fromPropertyDescriptor(prop).required(false).build()

}
