/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import com.github.benmanes.caffeine.cache.{RemovalCause, RemovalListener}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.util.StandardValidators
import org.locationtech.geomesa.utils.io.{CloseWithLogging, IsCloseable}

package object mixins {

  object Properties {

    val NifiBatchSize: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("BatchSize")
          .required(false)
          .description("Number of FlowFiles to execute in a single batch")
          .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .defaultValue("5")
          .build()
  }

  class CloseableRemovalListener[C : IsCloseable] extends RemovalListener[AnyRef, C]() {
    override def onRemoval(key: AnyRef, value: C, cause: RemovalCause): Unit = {
      if (cause.wasEvicted()) {
        CloseWithLogging(value)
      }
    }
  }
}
