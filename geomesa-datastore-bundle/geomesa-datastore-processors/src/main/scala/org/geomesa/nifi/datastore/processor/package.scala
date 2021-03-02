/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore

import org.apache.nifi.components.ValidationResult
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.Relationship

package object processor {

  /**
   * Full name of a flow file
   *
   * @param f flow file
   * @return
   */
  def fullName(f: FlowFile): String = f.getAttribute("path") + f.getAttribute("filename")

  /**
   * Create a validation result to mark a value invalid
   *
   * @param message message
   * @return
   */
  def invalid(message: String): ValidationResult = new ValidationResult.Builder().input(message).build()

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
}
