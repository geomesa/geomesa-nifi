/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.mixins

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.nifi.annotation.behavior.{ReadsAttribute, ReadsAttributes}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
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

  private var sftName: PropertyDescriptor = _

  private val sftCache = Caffeine.newBuilder().build(
    new CacheLoader[SftArgs, Either[Throwable, SimpleFeatureType]]() {
      override def load(key: SftArgs): Either[Throwable, SimpleFeatureType] =
        SftArgResolver.getArg(key).right.map(decorate)
    }
  )

  override protected def getPrimaryProperties: Seq[PropertyDescriptor] = {
    sftName = FeatureTypeProcessor.Properties.sftName(SimpleFeatureTypeLoader.listTypeNames)
    super.getPrimaryProperties ++ Seq(sftName, SftSpec, FeatureNameOverride)
  }

  /**
   * Load the configured feature type, based on processor and flow file properties
   *
   * @param context context
   * @param file flow file
   * @return
   */
  protected def loadFeatureType(context: ProcessContext, file: FlowFile): SimpleFeatureType =
    loadFeatureType(context, file, loadFeatureTypeName(context, file))

  /**
   * Load the configured feature type, based on processor and flow file properties
   *
   * @param context context
   * @param file flow file
   * @param name feature type name override
   * @return
   */
  protected def loadFeatureType(context: ProcessContext, file: FlowFile, name: Option[String]): SimpleFeatureType = {
    val specArg =
      Option(file.getAttribute(SftSpecAttribute))
          .orElse(BaseProcessor.getFirst(context, Seq(sftName, SftSpec)))
          .getOrElse {
            throw new IllegalArgumentException(
              s"SimpleFeatureType not specified: configure '$SftNameKey', 'SftSpec' " +
                  s"or flow-file attribute '$SftSpecAttribute'")
          }
    val nameArg = name.orNull
    sftCache.get(SftArgs(specArg, nameArg)).right.get // will throw an error if can't be loaded
  }

  /**
   * Load the configured feature type name override, based on processor and flow file properties
   *
   * @param context context
   * @param file flow file
   * @return
   */
  protected def loadFeatureTypeName(context: ProcessContext, file: FlowFile): Option[String] = {
    Option(file.getAttribute(SftNameAttribute))
        .orElse(Option(context.getProperty(FeatureNameOverride).evaluateAttributeExpressions().getValue))
  }
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
