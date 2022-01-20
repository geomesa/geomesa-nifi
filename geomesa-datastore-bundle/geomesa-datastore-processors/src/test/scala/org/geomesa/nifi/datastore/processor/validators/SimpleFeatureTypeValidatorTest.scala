/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.validators

import org.apache.nifi.components.ValidationContext
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito._

class SimpleFeatureTypeValidatorTest {

  private val validInputs = List(
    "geomesa {sfts {twitter = {fields = [{name = text, type = String}{name = username, type = String}{name = geom, type = Point, srid = 4326}]}}}",
    "geomesa { sfts { twitter = {fields = []}} }"
  )

  private val invalidInputs = List(
    "",
    "dsjhgjkdsfhgkjfdshgjisfkh",
    "geomesa { sfts {} }",
    "geomesa { sfts { twitter = {}} }"
  )

  @Test
  def validTests() : Unit = {
    val c = mock(classOf[ValidationContext])

    validInputs.foreach{ s =>
      val result = SimpleFeatureTypeValidator.validate("testThing", s, c)
      assertTrue("\"" + s + "\" should have been valid" , result.isValid)
    }
  }

  @Test
  def invalidTests() : Unit = {
    val c = mock(classOf[ValidationContext])

    invalidInputs.foreach{ s =>
      val result = SimpleFeatureTypeValidator.validate("testThing", s, c)
      assertFalse("\"" + s + "\" should have been invalid" , result.isValid)
    }
  }

}
