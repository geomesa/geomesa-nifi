/***********************************************************************
 * Copyright (c) 2015-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.validators

import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import org.apache.nifi.components.{ValidationContext, ValidationResult, Validator}
import org.geomesa.nifi.datastore.processor.mixins.ConvertInputProcessor
import org.locationtech.geomesa.metrics.core.ReporterFactory

import scala.util.control.NonFatal

/**
  * Simple validator of a convert spec. Tries to parse it to make sure that it can.
  */
object ConverterMetricsValidator extends Validator {

  import scala.collection.JavaConverters._

  private val DisableStart = ConfigFactory.parseString("""{interval:"-1 seconds"}""")

  override def validate(subject: String, input: String, context: ValidationContext): ValidationResult = {
    try {
      val registry = new MetricRegistry()
      val reporters = ConvertInputProcessor.parseReporterOptions(input).getConfig("options").getConfigList("reporters")
      reporters.asScala.foreach(conf => ReporterFactory(DisableStart.withFallback(conf), registry).close())
      new ValidationResult.Builder().subject(subject).input(input).valid(true).build()
    } catch {
      case NonFatal(e) =>
        new ValidationResult.Builder().subject(subject)
            .explanation(s"'$subject' is not a valid metric reporter: $e").input(input).build()
    }
  }
}
