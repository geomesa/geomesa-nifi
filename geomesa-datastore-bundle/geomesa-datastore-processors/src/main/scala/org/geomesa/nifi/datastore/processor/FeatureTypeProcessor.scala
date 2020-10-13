/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.nifi.annotation.behavior.{ReadsAttribute, ReadsAttributes}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.AbstractDataStoreProcessor.Writers
import org.geomesa.nifi.datastore.processor.validators.SimpleFeatureTypeValidator
import org.geotools.data._
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

/**
  * Mixin trait for configuring feature types via typesafe config
  */
@ReadsAttributes(
  Array(
    new ReadsAttribute(attribute = "geomesa.sft.name", description = "GeoMesa SimpleFeatureType name"),
    new ReadsAttribute(attribute = "geomesa.sft.spec", description = "GeoMesa SimpleFeatureType specification")
  )
)
trait FeatureTypeProcessor extends AbstractDataStoreProcessor {

  import FeatureTypeProcessor.Attributes
  import FeatureTypeProcessor.Properties.{FeatureNameOverride, SftNameKey, SftSpec}

  private var sftName: PropertyDescriptor = _

  override protected def getProcessorProperties: Seq[PropertyDescriptor] = {
    sftName = FeatureTypeProcessor.Properties.sftName(SimpleFeatureTypeLoader.listTypeNames)
    Seq(sftName, SftSpec, FeatureNameOverride) ++ super.getProcessorProperties
  }

  override protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: Writers): IngestProcessor = {
    val sftArg = FeatureTypeProcessor.getFirst(context, Seq(sftName, SftSpec))
    val typeName = Option(context.getProperty(FeatureNameOverride).evaluateAttributeExpressions().getValue)
    createIngest(context, dataStore, writers, sftArg, typeName)
  }

  protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: Writers,
      sftArg: Option[String],
      typeName: Option[String]): IngestProcessor

  /**
   * Log an error from writing a given feature
   *
   * @param sf feature
   * @param e error
   */
  protected def logError(sf: SimpleFeature, e: Throwable): Unit =
    logger.error(s"Error writing feature to store: '${DataUtilities.encodeFeature(sf)}'", e)

  /**
   * Abstraction over ingest methods
   *
   * @param store data store
   * @param writers feature writers
   * @param spec simple feature spec
   * @param name simple feature name override
   */
  abstract class IngestProcessorWithSchema(
      store: DataStore,
      writers: Writers,
      spec: Option[String],
      name: Option[String]
    ) extends IngestProcessor(store, writers) {

    private val sftCache = Caffeine.newBuilder().build(
      new CacheLoader[SftArgs, Either[Throwable, SimpleFeatureType]]() {
        override def load(key: SftArgs): Either[Throwable, SimpleFeatureType] = {
          SftArgResolver.getArg(key).right.map(decorate).right.flatMap { sft =>
            try {
              checkSchema(sft)
              Right(sft)
            } catch {
              case NonFatal(e) => Left(e)
            }
          }
        }
      }
    )

    override def ingest(
        context: ProcessContext,
        session: ProcessSession,
        file: FlowFile,
        flowFileName: String): (Long, Long) = {
      val sftSpec = Option(file.getAttribute(Attributes.SftSpecAttribute)).orElse(spec).getOrElse {
        throw new IllegalArgumentException(
          s"SimpleFeatureType not specified: configure '$SftNameKey', 'SftSpec' " +
              s"or flow-file attribute '${Attributes.SftSpecAttribute}'")
      }
      val sftName = Option(file.getAttribute(Attributes.SftNameAttribute)).orElse(this.name).orNull

      val sft = sftCache.get(SftArgs(sftSpec, sftName)) match {
        case Left(e) => throw e
        case Right(s) => s
      }

      val writer = writers.borrowWriter(sft.getTypeName)
      try { ingest(session, file, flowFileName, sft, writer) } finally {
        writers.returnWriter(writer)
      }
    }

    /**
     * Ingest a flow file
     *
     * @param session session
     * @param file flow file
     * @param name file name
     * @param sft simple feature type
     * @param fw feature writer
     * @return (success count, failure count)
     */
    protected def ingest(
        session: ProcessSession,
        file: FlowFile,
        name: String,
        sft: SimpleFeatureType,
        fw: SimpleFeatureWriter): (Long, Long)
  }
}

object FeatureTypeProcessor {

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

  def getFirst(context: ProcessContext, props: Seq[PropertyDescriptor]): Option[String] =
    props.toStream.flatMap(p => Option(context.getProperty(p).getValue)).headOption

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

    val ConverterAttribute = "geomesa.converter"
    val SftNameAttribute   = "geomesa.sft.name"
    val SftSpecAttribute   = "geomesa.sft.spec"

    val all: Seq[String] = Seq(ConverterAttribute, SftNameAttribute, SftSpecAttribute)
  }
}
