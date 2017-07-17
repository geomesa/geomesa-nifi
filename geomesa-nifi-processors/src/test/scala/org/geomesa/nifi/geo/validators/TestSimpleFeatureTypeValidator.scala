package org.geomesa.nifi.geo.validators

import org.apache.nifi.components.ValidationContext
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito._

class TestSimpleFeatureTypeValidator {

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
    val v = SimpleFeatureTypeValidator()
    val c = mock(classOf[ValidationContext])

    validInputs.foreach{ s =>
      val result = v.validate("testThing", s, c)
      assertTrue("\"" + s + "\" should have been valid" , result.isValid)
    }
  }

  @Test
  def invalidTests() : Unit = {
    val v = SimpleFeatureTypeValidator()
    val c = mock(classOf[ValidationContext])

    invalidInputs.foreach{ s =>
      val result = v.validate("testThing", s, c)
      assertFalse("\"" + s + "\" should have been invalid" , result.isValid)
    }
  }

}
