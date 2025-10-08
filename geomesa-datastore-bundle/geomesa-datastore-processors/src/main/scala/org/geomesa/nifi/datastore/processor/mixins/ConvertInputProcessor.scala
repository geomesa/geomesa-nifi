/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor
package mixins

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, ObjectPool, PooledObject}
import org.apache.nifi.annotation.lifecycle._
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.mixins.ConvertInputProcessor.ConverterCacheKey
import org.geomesa.nifi.datastore.processor.validators.{ConverterMetricsValidator, ConverterValidator}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.InputStream
import scala.util.control.NonFatal

/**
  * Converter processor for parsing simple features
  */
trait ConvertInputProcessor extends FeatureTypeProcessor {

  import ConvertInputProcessor.Attributes.ConverterAttribute
  import ConvertInputProcessor.Properties._
  import ConvertInputProcessor.{ConverterCallback, ConverterPool}

  import scala.collection.JavaConverters._

  private var converterName: PropertyDescriptor = _

  private val converterCache = Caffeine.newBuilder().build[ConverterCacheKey, Either[Throwable, ConverterPool]](
    new CacheLoader[ConverterCacheKey, Either[Throwable, ConverterPool]]() {
      override def load(key: ConverterCacheKey): Either[Throwable, ConverterPool] = {
        ConverterConfigResolver.getArg(ConfArgs(key.config)).right.flatMap { base =>
          try {
            val config = {
              val error = key.errorMode.map(m => ConfigFactory.parseString(s"options.error-mode = $m"))
              val reporters = key.reporters.map(ConvertInputProcessor.parseReporterOptions)
              error.getOrElse(ConfigFactory.empty())
                  .withFallback(reporters.getOrElse(ConfigFactory.empty()))
                  .withFallback(base)
            }
            val factory = new BasePooledObjectFactory[SimpleFeatureConverter] {
              override def create(): SimpleFeatureConverter = SimpleFeatureConverter(key.sft, config)
              override def wrap(obj: SimpleFeatureConverter): PooledObject[SimpleFeatureConverter] =
                new DefaultPooledObject(obj)
              override def destroyObject(p: PooledObject[SimpleFeatureConverter]): Unit = p.getObject.close()
            }

            val poolConfig = new GenericObjectPoolConfig[SimpleFeatureConverter]()
            poolConfig.setMaxTotal(-1)

            Right(new GenericObjectPool(factory, poolConfig))
          } catch {
            case NonFatal(e) => Left(e)
          }
        }
      }
    }
  )

  override protected def getSecondaryProperties: Seq[PropertyDescriptor] = {
    converterName = ConvertInputProcessor.Properties.converterName(ConverterConfigLoader.listConverterNames)
    super.getSecondaryProperties ++
        Seq(converterName, ConverterSpec, ConverterErrorMode, ConvertFlowFileAttributes, ConverterMetricReporters)
  }


  protected def convert(
      context: ProcessContext,
      session: ProcessSession,
      file: FlowFile,
      sft: SimpleFeatureType,
      callback: ConverterCallback): IngestResult = {

    val config =
      Option(file.getAttribute(ConverterAttribute))
          .orElse(BaseProcessor.getFirst(context, Seq(converterName, ConverterSpec)))
          .getOrElse {
            throw new IllegalArgumentException(
              s"Converter not specified: configure '$ConverterNameKey', '${ConverterSpec.getName}' " +
                  s"or flow-file attribute '$ConverterAttribute'")
          }

    val errorMode = Option(context.getProperty(ConverterErrorMode).evaluateAttributeExpressions().getValue)
    val reporters = Option(context.getProperty(ConverterMetricReporters).getValue)
    val attributes = Option(context.getProperty(ConvertFlowFileAttributes).asBoolean()).exists(_.booleanValue())

    val converters = converterCache.get(ConverterCacheKey(sft, config, errorMode, reporters)) match {
      case Right(c) => c
      case Left(e) => throw e
    }

    val converter = converters.borrowObject()
    try {
      val globalParams = {
        val inputFile = EvaluationContext.inputFileParam(fullName(file))
        if (attributes) {
          inputFile ++ (file.getAttributes.asScala -- ConvertInputProcessor.AllAttributes)
        } else {
          inputFile
        }
      }
      // create new counters so we don't use any shared state
      val ec = converter.createEvaluationContext(globalParams)
      var failed = 0L
      session.read(file, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          WithClose(converter.process(in, ec)) { iter =>
            failed += callback.apply(iter)
          }
        }
      })
      IngestResult(ec.stats.success(0) - failed, ec.stats.failure(0) + failed)
    } finally {
      converters.returnObject(converter)
    }
  }

  // noinspection ScalaUnusedSymbol
  @OnRemoved
  @OnStopped
  @OnShutdown
  def closeConverterCache(): Unit = {
    CloseWithLogging(converterCache.asMap.asScala.values.flatMap(_.right.toSeq))
    converterCache.invalidateAll()
  }
}


object ConvertInputProcessor {

  type ConverterPool = ObjectPool[SimpleFeatureConverter]

  private val AllAttributes =
    Seq(
      FeatureTypeProcessor.Attributes.SftNameAttribute,
      FeatureTypeProcessor.Attributes.SftSpecAttribute,
      Attributes.ConverterAttribute
    )

  def parseReporterOptions(reporters: String): Config = {
    val i = reporters.indexWhere(c => !Character.isWhitespace(c))
    val config = if (i == -1) {
      reporters
    } else {
      reporters.charAt(i) match {
        case '[' => s"options.reporters = $reporters"
        case '{' => s"options.reporters = [\n$reporters\n]"
        case _ => reporters
      }
    }
    ConfigFactory.parseString(config).resolve()
  }

  object Properties {

    val ConverterNameKey = "ConverterName"

    def converterName(values: Seq[String]): PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name(ConverterNameKey)
          .required(false)
          .description("Choose an SimpleFeature Converter defined by a GeoMesa SFT Provider (preferred)")
          .allowableValues(values.sorted: _*)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build()

    val ConverterSpec: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("ConverterSpec")
          .required(false)
          .description("Manually define a converter using typesafe config")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .addValidator(ConverterValidator)
          .build()

    val ConverterErrorMode: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("ConverterErrorMode")
          .required(false)
          .description("Override the converter error mode behavior")
          .allowableValues(ErrorMode.LogErrors.toString, ErrorMode.RaiseErrors.toString, /*deprecated*/ "skip-bad-records")
          .expressionLanguageSupported(EnvironmentOrRegistry)
          .build()

    @deprecated("Use ConverterMetricsRegistry")
    val ConverterMetricReporters: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("ConverterMetricReporters")
          .displayName("ConverterMetricReporters (deprecated)")
          .required(false)
          .description("Override the converter metrics reporters. This property is deprecated - use ConverterMetricsRegistry instead")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .addValidator(ConverterMetricsValidator)
          .build()

    val ConvertFlowFileAttributes: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("ConvertFlowFileAttributes")
          .required(false)
          .description("Expose flow file attributes to the converter framework by name")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .allowableValues("true", "false")
          .defaultValue("false")
          .build()
  }

  object Attributes {
    val ConverterAttribute = "geomesa.converter"
  }

  /**
   * Callback for reading features converted from an input flowfile
   */
  trait ConverterCallback {
    /**
     * Callback for a converted feature
     *
     * @param features features
     * @return count of 'failed' features
     */
    def apply(features: Iterator[SimpleFeature]): Long
  }

  private case class ConverterCacheKey(
      sft: SimpleFeatureType, config: String, errorMode: Option[String], reporters: Option[String])
}
