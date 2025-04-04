/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.components.resource.{ResourceCardinality, ResourceType}
import org.apache.nifi.components.{PropertyDescriptor, ValidationResult}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.util.StandardValidators

import java.io.File
import scala.util.control.NonFatal

package object processor extends LazyLogging {

  private val ExtraClasspathsEnv = "GEOMESA_EXTRA_CLASSPATHS"

  val ExtraClasspaths: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("ExtraClasspaths")
        .required(false)
        .description("Add additional resources to the classpath")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .dynamicallyModifiesClasspath(true)
        .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
        .defaultValue(s"$${$ExtraClasspathsEnv}")
        .build()

  // there's no way to skip validation when using an env var for the default model dir, so we need to ensure
  // that the directory exists
  if (!sys.env.contains(ExtraClasspathsEnv) && !sys.props.contains(ExtraClasspathsEnv)) {
    val tmpDir = sys.props("java.io.tmpdir")
    val defaultModelDir = s"$tmpDir/nifi-models/"
    val dir = new File(defaultModelDir)
    try {
      if (dir.exists() || dir.mkdirs()) {
        System.setProperty(ExtraClasspathsEnv, defaultModelDir)
      } else {
        throw new RuntimeException(s"Could not create directory '$defaultModelDir'")
      }
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error creating default model directory '${dir.getPath}', defaulting to '$tmpDir':", e)
        System.setProperty(ExtraClasspathsEnv, tmpDir)
    }
  }

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
