/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.validators

import org.apache.nifi.components.{ValidationContext, ValidationResult, Validator}

import scala.util.{Success, Try}

/**
  * Validates gzip level, between 1-9
  */
object GzipLevelValidator extends Validator {
  override def validate(subject: String, input: String, context: ValidationContext): ValidationResult = {
    val builder = new ValidationResult.Builder().subject(subject).input(input)
    if (input == null || input.isEmpty) {
      builder.valid(true).build()
    } else if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
      builder.explanation("Expression Language Present").valid(true).build()
    } else {
      Try(input.toInt) match {
        case Success(i) if i > 0 && i < 10 => builder.valid(true).build()
        case _ => builder.explanation("Input must be an integer between 1 and 9").valid(false).build()
      }
    }
  }
}
