/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore

import org.apache.nifi.components.{PropertyDescriptor, ValidationResult}
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.Relationship

package object processor {

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

  def evaluateDescriptors(
      descriptors: Seq[PropertyDescriptor],
      context: PropertyContext): Map[PropertyDescriptor, String] = {
    evaluateDescriptors(descriptors, context, null: FlowFile)
  }

  def evaluateDescriptors(
      descriptors: Seq[PropertyDescriptor],
      context: PropertyContext,
      file: FlowFile): Map[PropertyDescriptor, String] = {
    descriptors.flatMap(descriptor => evaluateDescriptor(descriptor, context, file).map(descriptor -> _)).toMap
  }

  def evaluateDescriptors(
      descriptors: Seq[PropertyDescriptor],
      context: PropertyContext,
      variables: java.util.Map[String, String]): Map[PropertyDescriptor, String] = {
    descriptors.flatMap(descriptor => evaluateDescriptor(descriptor, context, variables).map(descriptor -> _)).toMap
  }

  def evaluateDescriptor(descriptor: PropertyDescriptor, context: PropertyContext): Option[String] =
    evaluateDescriptor(descriptor, context, null: FlowFile)

  def evaluateDescriptor(descriptor: PropertyDescriptor, context: PropertyContext, file: FlowFile): Option[String] = {
    evaluateDescriptor(descriptor, context, Left(file))
  }

  def evaluateDescriptor(
      descriptor: PropertyDescriptor,
      context: PropertyContext,
      variables: java.util.Map[String, String]): Option[String] = {
    evaluateDescriptor(descriptor, context, Right(variables))
  }


  private def evaluateDescriptor(
      descriptor: PropertyDescriptor,
      context: PropertyContext,
      fileOrVariables: Either[FlowFile, java.util.Map[String, String]]): Option[String] = {
    val prop = context.getProperty(descriptor)
    if (!prop.isSet || descriptor.getControllerServiceDefinition != null) { None } else {
      val evaluated = descriptor.getExpressionLanguageScope match {
        case ExpressionLanguageScope.NONE => prop
        case ExpressionLanguageScope.VARIABLE_REGISTRY => prop.evaluateAttributeExpressions()
        case ExpressionLanguageScope.FLOWFILE_ATTRIBUTES =>
          fileOrVariables match {
            case Left(file)  => prop.evaluateAttributeExpressions(file)
            case Right(vars) => prop.evaluateAttributeExpressions(vars)
          }
        case scope => throw new NotImplementedError(s"Unexpected expression language scope: $scope")
      }
      Option(evaluated.getValue).filter(_.nonEmpty)
    }
  }

  object Relationships {
    val SuccessRelationship: Relationship = new Relationship.Builder().name("success").description("Success").build()
    val FailureRelationship: Relationship = new Relationship.Builder().name("failure").description("Failure").build()
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
