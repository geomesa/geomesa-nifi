package org.geomesa.nifi.geo.validators

import org.apache.nifi.components.{ValidationContext, ValidationResult, Validator}
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigResolver}

/**
  * Simple validator of a convert spec. Tries to parse it to make sure that it can.
  */
object ConverterValidator extends Validator {
  override def validate(subject: String, input: String, context: ValidationContext): ValidationResult =
    ConverterConfigResolver.getArg(ConfArgs(input)) match {
      case Left(_) =>
        new ValidationResult.Builder().subject(subject)
          .explanation(s"'$subject' is not a valid converter").input(input).build()
      case Right(_) =>
        new ValidationResult.Builder().subject(subject).input(input).valid(true).build()
    }
}
