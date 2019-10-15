/***********************************************************************
 * Copyright (c) 2015-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.validators

import org.apache.nifi.components.{ValidationContext, ValidationResult, Validator}
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}

/**
  * Simple validator that tries to parse a given simple feature type to check that it is valid.
  */
object SimpleFeatureTypeValidator extends Validator {
  override def validate(subject: String, input: String, validationContext: ValidationContext): ValidationResult = {
    val builder = new ValidationResult.Builder().subject(subject).input(input)
    SftArgResolver.getArg(SftArgs(input, null)) match {
      case Left(e)  => builder.explanation(s"'$subject' is not a valid simple feature type: $e").build();
      case Right(_) => builder.valid(true).build();
    }
  }
}
