package org.geomesa.nifi.geo.validators

import org.apache.nifi.components.{ValidationContext, ValidationResult, Validator}

class KnownNameOrExpressionValidator(names: List[String]) extends Validator{
  override def validate(
                         subject: String,
                         input: String,
                         validationContext: ValidationContext): ValidationResult =
    if (
      validationContext.isExpressionLanguageSupported(subject) &&
      validationContext.isExpressionLanguagePresent(input) ||
      names.contains(input)
    )
      new ValidationResult.Builder().subject(subject).input(input).valid(true).build
    else
      new ValidationResult.Builder()
        .subject(subject)
        .explanation(s"'$subject' is neither a known name nor an expression.")
        .input(input).build
}

object KnownNameOrExpressionValidator {
  def apply(names:List[String]) = new KnownNameOrExpressionValidator(names)
}