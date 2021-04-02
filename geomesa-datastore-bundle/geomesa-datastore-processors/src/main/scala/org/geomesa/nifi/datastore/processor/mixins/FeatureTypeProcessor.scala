/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor
package mixins

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.nifi.annotation.behavior.{ReadsAttribute, ReadsAttributes}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.validators.SimpleFeatureTypeValidator
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Mixin trait for configuring feature types via typesafe config
  */
@ReadsAttributes(
  Array(
    new ReadsAttribute(attribute = "geomesa.sft.name", description = "GeoMesa SimpleFeatureType name"),
    new ReadsAttribute(attribute = "geomesa.sft.spec", description = "GeoMesa SimpleFeatureType specification")
  )
)
trait FeatureTypeProcessor extends BaseProcessor {

  import FeatureTypeProcessor.Attributes.{SftNameAttribute, SftSpecAttribute}
  import FeatureTypeProcessor.Properties.{FeatureNameOverride, SftNameKey, SftSpec}

  import scala.collection.JavaConverters._

  private var sftName: PropertyDescriptor = _

  private val sftCache = Caffeine.newBuilder().build(
    new CacheLoader[(SftArgs, Map[String, String]), Either[Throwable, SimpleFeatureType]]() {
      override def load(key: (SftArgs, Map[String, String])): Either[Throwable, SimpleFeatureType] = {
        val sft = SftArgResolver.getArg(key._1)
        decorator match {
          case None => sft
          case Some(d) =>
            val props = key._2.map { case (k, v) =>
              getSupportedPropertyDescriptors.asScala.find(_.getName == k).get -> v
            }
            sft.right.map(d.decorate(_, props))
        }
      }
    }
  )

  override protected def getPrimaryProperties: Seq[PropertyDescriptor] = {
    sftName = FeatureTypeProcessor.Properties.sftName(SimpleFeatureTypeLoader.listTypeNames)
    super.getPrimaryProperties ++ Seq(sftName, SftSpec, FeatureNameOverride)
  }

  /**
   * Load the configured feature type, based on processor and flow file properties
   *
   * @param properties context properties
   * @param file flow file
   * @return
   */
  protected def loadFeatureType(properties: Map[PropertyDescriptor, String], file: FlowFile): SimpleFeatureType =
    loadFeatureType(properties, file, loadFeatureTypeName(properties, file))

  /**
   * Load the configured feature type, based on processor and flow file properties
   *
   * @param properties context properties
   * @param file flow file
   * @param name feature type name override
   * @return
   */
  protected def loadFeatureType(
      properties: Map[PropertyDescriptor, String],
      file: FlowFile,
      name: Option[String]): SimpleFeatureType = {
    val specArg =
      Option(file.getAttribute(SftSpecAttribute))
          .orElse(properties.get(sftName))
          .orElse(properties.get(SftSpec))
          .getOrElse {
            throw new IllegalArgumentException(
              s"SimpleFeatureType not specified: configure '$SftNameKey', 'SftSpec' " +
                  s"or flow-file attribute '$SftSpecAttribute'")
          }
    val nameArg = name.orNull
    val decoration = decorator.map(d => properties.filterKeys(d.properties.contains)).getOrElse(Map.empty)
    val key = (SftArgs(specArg, nameArg), decoration.map { case (k, v) => k.getName -> v })
    sftCache.get(key).right.get // will throw an error if can't be loaded
  }

  /**
   * Load the configured feature type name override, based on processor and flow file properties
   *
   * @param properties context properties
   * @param file flow file
   * @return
   */
  protected def loadFeatureTypeName(properties: Map[PropertyDescriptor, String], file: FlowFile): Option[String] =
    Option(file.getAttribute(SftNameAttribute)).orElse(properties.get(FeatureNameOverride))
}

object FeatureTypeProcessor {

  /**
   * Processor configuration properties
   */
  object Properties {

    val SftNameKey = "SftName"

    def sftName(values: Seq[String]): PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name(SftNameKey)
          .required(false)
          .description("Choose a simple feature type defined by a GeoMesa SFT Provider (preferred)")
          .allowableValues(values.sorted: _*)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build()

    val SftSpec: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("SftSpec")
          .required(false)
          .description("Manually define a SimpleFeatureType (SFT) config spec")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .addValidator(SimpleFeatureTypeValidator)
          .build()

    val FeatureNameOverride: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("FeatureNameOverride")
          .required(false)
          .description("Override the Simple Feature Type name from the SFT Spec")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .build()
  }

  object Attributes {
    val SftNameAttribute = "geomesa.sft.name"
    val SftSpecAttribute = "geomesa.sft.spec"
  }
}
