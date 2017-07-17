package org.geomesa.nifi.geo.validators

import org.apache.nifi.components.{ValidationContext, ValidationResult, Validator}
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}

/**
  * Simple validator that tries to parse a given simple feature type to check that it is valid.
  */
class SimpleFeatureTypeValidator extends Validator {
  override def validate(subject: String, input: String, validationContext: ValidationContext): ValidationResult = {
    SftArgResolver.getArg(SftArgs(input, null)) match {
      case Left(_) => new ValidationResult.Builder().subject(subject).explanation(String.format("'%s' is not a valid simple feature type", subject)).input(input).build;
      case Right(_) => new ValidationResult.Builder().subject(subject).input(input).valid(true).build;
    }
  }
}


object SimpleFeatureTypeValidator {
  def apply() : SimpleFeatureTypeValidator = new SimpleFeatureTypeValidator()
}
