package org.geomesa.nifi.geo.validators

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
