/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.mixins

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.validators.SimpleFeatureTypeValidator
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.geotools._

/**
  * Mixin trait for configuring feature types via typesafe config
  */
trait FeatureTypeProcessor extends UserDataProcessor {

  import FeatureTypeProcessor.Attributes.{SftNameAttribute, SftSpecAttribute}
  import FeatureTypeProcessor.Properties.{FeatureNameOverride, SftNameKey, SftSpec}

  private var sftName: PropertyDescriptor = _

  private val sftCache = Caffeine.newBuilder().build[SftArgs, Either[Throwable, SimpleFeatureType]](
    new CacheLoader[SftArgs, Either[Throwable, SimpleFeatureType]]() {
      override def load(key: SftArgs): Either[Throwable, SimpleFeatureType] = SftArgResolver.getArg(key)
    }
  )

  override protected def getSecondaryProperties: Seq[PropertyDescriptor] = {
    sftName = FeatureTypeProcessor.Properties.sftName(SimpleFeatureTypeLoader.listTypeNames)
    Seq(sftName, SftSpec, FeatureNameOverride) ++ super.getSecondaryProperties
  }

  override protected def customValidate(context: ValidationContext): java.util.Collection[ValidationResult] = {
    val results = new java.util.HashSet(super.customValidate(context))

    val spec = context.getProperty(SftSpec).getValue
    if (spec != null && spec.nonEmpty) {
      val name = context.getProperty(FeatureNameOverride).evaluateAttributeExpressions().getValue
      SftArgResolver.getArg(SftArgs(spec, name)).left.foreach { e =>
        val builder =
          new ValidationResult.Builder()
              .subject(SftSpec.getName)
              .input(spec)
              .explanation(s"'${SftSpec.getName}' is not a valid simple feature type: $e")
        results.add(builder.build())
      }
    }

    results
  }

  /**
   * Load the configured feature type, based on processor and flow file properties
   *
   * @param context context
   * @param file flow file
   * @param spec simple feature type spec or name
   * @param name simple feature type name override
   * @return
   */
  protected def loadFeatureType(
      context: ProcessContext,
      file: FlowFile,
      spec: Option[String] = None,
      name: Option[String] = None): SimpleFeatureType = {
    val specArg = spec.orElse(loadFeatureTypeSpec(context, file)).getOrElse {
      throw new IllegalArgumentException(
        s"SimpleFeatureType not specified: configure '$SftNameKey', 'SftSpec' " +
            s"or flow-file attribute '$SftSpecAttribute'")
    }
    val nameArg = name.orElse(loadFeatureTypeName(context, file)).orNull
    val sft = sftCache.get(SftArgs(specArg, nameArg)) match {
      case Right(sft) => sft
      case Left(e) =>
        throw new IllegalArgumentException(s"Unable to load feature type '$specArg' for file ${file.getId}:", e)
    }
    val userData = loadFeatureTypeUserData(sft, context, file)
    if (userData.isEmpty) { sft } else {
      val copy = SimpleFeatureTypes.copy(sft)
      copy.getUserData.putAll(userData)
      copy
    }
  }

  protected def loadFeatureTypeSpec(context: ProcessContext, file: FlowFile): Option[String] = {
    Option(file.getAttribute(SftSpecAttribute))
        .orElse(BaseProcessor.getFirst(context, Seq(sftName, SftSpec)))
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
          .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
          .build()
  }

  object Attributes {
    val SftNameAttribute = "geomesa.sft.name"
    val SftSpecAttribute = "geomesa.sft.spec"
  }
}
