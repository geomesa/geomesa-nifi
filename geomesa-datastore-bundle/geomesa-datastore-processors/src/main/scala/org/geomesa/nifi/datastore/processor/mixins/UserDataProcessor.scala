/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.mixins

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.opengis.feature.simple.SimpleFeatureType

import java.io.StringReader
import java.util.{Collections, Properties}

/**
  * Mixin trait for configuring feature types user data
  */
trait UserDataProcessor extends BaseProcessor {

  import UserDataProcessor.Properties.SftUserData

  override protected def getPrimaryProperties: Seq[PropertyDescriptor] =
    super.getPrimaryProperties ++ Seq(SftUserData)

  /**
   * Load the configured user data, based on processor and flow file properties
   *
   * @param sft simple feature type to decorate
   * @param context context
   * @param file file
   */
  protected def loadFeatureTypeUserData(
      sft: SimpleFeatureType,
      context: ProcessContext,
      file: FlowFile): java.util.Map[String, String] = {
    val userData = context.getProperty(SftUserData).evaluateAttributeExpressions(file).getValue
    if (userData == null || userData.isEmpty) { new java.util.HashMap[String, String]() } else {
      val props = new Properties()
      props.load(new StringReader(userData))
      props.asInstanceOf[java.util.Map[String, String]]
    }
  }
}

object UserDataProcessor {

  object Properties {
    val SftUserData: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("SftUserData")
          .required(false)
          .description("Add additional user data to the Simple Feature Type")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .defaultValue("${geomesa.sft.user-data}")
          .build()
  }
}
