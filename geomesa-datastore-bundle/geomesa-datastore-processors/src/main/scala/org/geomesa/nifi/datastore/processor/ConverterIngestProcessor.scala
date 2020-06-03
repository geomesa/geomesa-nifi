/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.datastore.processor

import java.io.InputStream
import java.util.Collections

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, ObjectPool, PooledObject}
import org.apache.nifi.annotation.behavior.{ReadsAttribute, ReadsAttributes}
import org.apache.nifi.annotation.lifecycle._
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.validators.ConverterValidator
import org.geotools.data._
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Converter ingest processor for geotools data stores
  */
@ReadsAttributes(
  Array(
    new ReadsAttribute(attribute = "geomesa.converter", description = "GeoMesa converter name or configuration")
  )
)
trait ConverterIngestProcessor extends AbstractGeoIngestProcessor {

  import ConverterIngestProcessor._
  import AbstractGeoIngestProcessor._

  import scala.collection.JavaConverters._

  private var converterName: PropertyDescriptor = _

  @OnAdded // reload on add to pick up any sft/converter classpath changes
  override def initDescriptors(): Unit = {
    converterName = ConverterIngestProcessor.converterName(ConverterConfigLoader.listConverterNames)
    super.initDescriptors()
  }

  override protected def getProcessorProperties: Seq[PropertyDescriptor] =
    Seq(converterName, ConverterSpec, ConverterErrorMode)

  override protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: Writers,
      sftArg: Option[String],
      typeName: Option[String]): IngestProcessor = {
    val converterArg = AbstractGeoIngestProcessor.getFirst(context, Seq(converterName, ConverterSpec))
    val errorMode = Option(context.getProperty(ConverterErrorMode).evaluateAttributeExpressions().getValue)
    new ConverterIngest(dataStore, writers, sftArg, typeName, converterArg, errorMode)
  }

  /**
   * Converter ingest
   *
   * @param store data store
   * @param writers feature writers
   * @param spec simple feature spec
   * @param name simple feature name override
   * @param conf converter config
   * @param error converter error mode
   */
  class ConverterIngest(
      store: DataStore,
      writers: Writers,
      spec: Option[String],
      name: Option[String],
      conf: Option[String],
      error: Option[String]
    ) extends IngestProcessor(store, writers, spec, name) {

    private val converterCache = Caffeine.newBuilder().build(
      new CacheLoader[(SimpleFeatureType, String), Either[Throwable, ObjectPool[SimpleFeatureConverter]]]() {
        override def load(key: (SimpleFeatureType, String)): Either[Throwable, ObjectPool[SimpleFeatureConverter]] = {
          ConverterConfigResolver.getArg(ConfArgs(key._2)).right.flatMap { base =>
            try {
              val config = error match {
                case None => base
                case Some(mode) =>
                  val opts = ConfigValueFactory.fromMap(Collections.singletonMap("error-mode", mode))
                  ConfigFactory.empty().withValue("options", opts).withFallback(base)
              }

              val factory = new BasePooledObjectFactory[SimpleFeatureConverter] {
                override def create(): SimpleFeatureConverter = SimpleFeatureConverter(key._1, config)
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

    override protected def ingest(
        session: ProcessSession,
        file: FlowFile,
        name: String,
        sft: SimpleFeatureType,
        fw: SimpleFeatureWriter): (Long, Long) = {

      val config = Option(file.getAttribute(Attributes.ConverterAttribute)).orElse(conf).getOrElse {
        throw new IllegalArgumentException(
          s"Converter not specified: configure '$ConverterNameKey', '${ConverterSpec.getName}' " +
              s"or flow-file attribute '${Attributes.ConverterAttribute}'")
      }

      val converters = converterCache.get(sft -> config) match {
        case Left(e) => throw e
        case Right(c) => c
      }

      val converter = converters.borrowObject()
      try {
        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(name))
        session.read(file, new InputStreamCallback {
          override def process(in: InputStream): Unit = {
            converter.process(in, ec).foreach { sf =>
              try { FeatureUtils.write(fw, sf) } catch {
                case NonFatal(e) =>
                  ec.success.inc(-1)
                  ec.failure.inc(1)
                  logError(sf, e)
              }
            }
          }
        })
        (ec.success.getCount, ec.failure.getCount)
      } finally {
        converters.returnObject(converter)
      }
    }

    override def close(): Unit = {
      try { super.close() } finally {
        CloseQuietly.raise(converterCache.asMap.asScala.values.flatMap(_.right.toSeq))
      }
    }
  }
}

object ConverterIngestProcessor {

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
        .allowableValues(ErrorMode.SkipBadRecords.toString, ErrorMode.RaiseErrors.toString)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build()

}
