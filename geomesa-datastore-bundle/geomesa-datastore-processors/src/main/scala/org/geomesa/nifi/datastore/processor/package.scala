/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore

import org.apache.nifi.components.resource.{ResourceCardinality, ResourceType}
import org.apache.nifi.components.{PropertyDescriptor, ValidationResult}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.util.StandardValidators

package object processor {

  // expression language scope that works with both nifi 2.x and 1.x
  // will be ENVIRONMENT in 2.x, and VARIABLE_REGISTRY in 1.x
  val EnvironmentOrRegistry: ExpressionLanguageScope = {
    try { ExpressionLanguageScope.valueOf("ENVIRONMENT") } catch {
      case _: IllegalArgumentException => ExpressionLanguageScope.valueOf("VARIABLE_REGISTRY") // nifi 1.x back compatibility
    }
  }

  val ExtraClasspaths: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("ExtraClasspaths")
        .required(false)
        .description("Add additional resources to the classpath")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(EnvironmentOrRegistry)
        .dynamicallyModifiesClasspath(true)
        .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
        .build()

  /**
   * Full name of a flow file
   *
   * @param f flow file
   * @return
   */
  def fullName(f: FlowFile): String = s"${f.getAttribute("path")}${f.getAttribute("filename")}"

  /**
   * Create a validation result to mark a value invalid
   *
   * @param message message
   * @return
   */
  def invalid(message: String): ValidationResult = new ValidationResult.Builder().input(message).build()

  def invalid(subject: String, error: Throwable, input: Option[String] = None): ValidationResult = {
    val builder = new ValidationResult.Builder().subject(subject)
    input.foreach(builder.input)
    val explanation = new StringBuilder
    var e = error
    while (e != null) {
      if (explanation.nonEmpty) {
        explanation.append("\n  Caused by: ")
      }
      explanation.append(e.toString)
      e = e.getCause
    }
    builder.explanation(explanation.toString).build()
  }

  object Relationships {
    val SuccessRelationship: Relationship  = new Relationship.Builder().name("success").description("Success").build()
    val FailureRelationship: Relationship  = new Relationship.Builder().name("failure").description("Failure").build()
    val OriginalRelationship: Relationship = new Relationship.Builder().name("original").description("Original input file").build()
  }

  /**
   * Compatibility mode for schemas
   *
   * * Exact - schemas must match exactly
   * * Existing - use the existing schema, any non-matching input schema fields will be dropped
   * * Update - update the schema to match the input schema
   */
  object CompatibilityMode extends Enumeration {
    type CompatibilityMode = Value
    val Exact, Existing, Update = Value
  }

  object Attributes {
    val IngestSuccessCount = "geomesa.ingest.successes"
    val IngestFailureCount = "geomesa.ingest.failures"
  }

  case class IngestResult(success: Long, failure: Long)
}
