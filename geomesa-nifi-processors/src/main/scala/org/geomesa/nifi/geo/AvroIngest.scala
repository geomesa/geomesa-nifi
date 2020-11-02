/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.geo

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.Converters
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object AvroIngest {

  import scala.collection.JavaConverters._

  val ExactMatch = "by attribute number and order"
  val LenientMatch = "by attribute name"

  val AvroMatchMode: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("Avro SFT match mode")
      .description("Determines how Avro SFT mismatches are handled")
      .required(false)
      .defaultValue(ExactMatch)
      .allowableValues(ExactMatch, LenientMatch)
      .build()

  val UseProvidedFid: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("Use provided feature ID")
      .description("Use the feature ID from the Avro record, or generate a new one")
      .required(false)
      .defaultValue("false")
      .allowableValues("true", "false")
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
      .build()

  /**
   * Creates an adapter from one SFT to another
   *
   * @param in input sft
   * @param out output sft
   * @return
   */
  def convert(in: SimpleFeatureType, out: SimpleFeatureType): SimpleFeature => SimpleFeature = {
    import RichSimpleFeatureType._
    val outGeometryLocalName = out.getGeomField
    val inToOut = out.getAttributeDescriptors.asScala.map { outAttributeDescriptor =>
      val outLocalName = outAttributeDescriptor.getLocalName
      in.indexOf(outLocalName) match {
        case inIndex if inIndex >= 0 =>
          val outTypeBinding = outAttributeDescriptor.getType.getBinding
          if (outTypeBinding.isAssignableFrom(in.getType(inIndex).getBinding)) {
            sf: SimpleFeature => sf.getAttribute(inIndex)
          } else {
            sf: SimpleFeature => Converters.convert(sf.getAttribute(inIndex), outTypeBinding).asInstanceOf[AnyRef]
          }
        case _ if outLocalName.equals(outGeometryLocalName) => sf: SimpleFeature => sf.getDefaultGeometry
        case _ => _: SimpleFeature => null
      }
    }
    sf: SimpleFeature => {
      val o = SimpleFeatureBuilder.build(out, inToOut.map(_(sf)).asJava, sf.getID)
      o.getUserData.putAll(sf.getUserData)
      o
    }
  }

  /**
   * Verifies the input type is compatible with the existing feature type in the data store
   *
   * Compatibility currently implies:
   *   1. feature type has the same or fewer number of attributes
   *   2. corresponding attributes have compatible type binding
   *
   * It does not imply:
   *   1. feature type has exact same number of attributes
   *   2. attributes have the same name (attribute number is used)
   *   3. attributes have the exact same binding
   *
   * @param existing current feature type
   * @param input input simple feature type
   */
  def checkCompatibleSchema(existing: SimpleFeatureType, input: SimpleFeatureType): Option[IllegalArgumentException] = {
    require(existing != null) // if we're calling this method the schema should have already been created

    lazy val exception =
      new IllegalArgumentException(
        "Input schema does not match existing type:" +
          s"\n  Input:    ${SimpleFeatureTypes.encodeType(input)}" +
          s"\n  Existing: ${SimpleFeatureTypes.encodeType(existing)}")

    if (input.getAttributeCount > existing.getAttributeCount) {
      return Some(exception)
    }

    var i = 0
    while (i < input.getAttributeCount) {
      if (!existing.getDescriptor(i).getType.getBinding.isAssignableFrom(input.getDescriptor(i).getType.getBinding)) {
        return Some(exception)
      }
      i += 1
    }

    None
  }

}

