package org.geomesa.nifi.geo.validators

import org.apache.nifi.components.{ValidationContext, ValidationResult, Validator}
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigResolver}

/**
  * Simple validator of a convert spec. Tries to parse it to make sure that it can.
  */
class ConverterValidator extends Validator{
  override def validate(subject: String, input: String, context: ValidationContext): ValidationResult = {
    ConverterConfigResolver.getArg(ConfArgs(input)) match {
      case Left(_) => new ValidationResult.Builder().subject(subject).explanation(String.format("'%s' is not a valid converter", subject)).input(input).build;
      case Right(_) => new ValidationResult.Builder().subject(subject).input(input).valid(true).build;
    }
  }
}

object ConverterValidator {
  def apply() : ConverterValidator = new ConverterValidator()
}

