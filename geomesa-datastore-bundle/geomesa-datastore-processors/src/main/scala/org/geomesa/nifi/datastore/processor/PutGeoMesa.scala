/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior._
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult, Validator}
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.geomesa.nifi.datastore.processor.CompatibilityMode.CompatibilityMode
import org.geomesa.nifi.datastore.processor.mixins.ConvertInputProcessor.ConverterCallback
import org.geomesa.nifi.datastore.processor.mixins.{ConvertInputProcessor, DataStoreIngestProcessor, FeatureWriters}
import org.geotools.data._
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.StringReader
import scala.util.control.NonFatal


@Tags(Array("geomesa", "geo", "ingest", "convert", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes(
  Array(
    new ReadsAttribute(attribute = "geomesa.converter", description = "GeoMesa converter name or configuration"),
    new ReadsAttribute(attribute = "geomesa.sft.name", description = "GeoMesa SimpleFeatureType name"),
    new ReadsAttribute(attribute = "geomesa.sft.spec", description = "GeoMesa SimpleFeatureType specification")
  )
)
@WritesAttributes(
  Array(
    new WritesAttribute(attribute = "geomesa.ingest.successes", description = "Number of features written successfully"),
    new WritesAttribute(attribute = "geomesa.ingest.failures", description = "Number of features with errors")
  )
)
@SupportsBatching
class PutGeoMesa extends DataStoreIngestProcessor with ConvertInputProcessor {

  import PutGeoMesa.Properties.InitSchemas

  override protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: FeatureWriters,
      mode: CompatibilityMode): IngestProcessor = {
    val ingest = new ConverterIngest(dataStore, writers, mode)
    // due to validation, should be all Rights
    ingest.init(PutGeoMesa.initSchemas(context).map { case Right(sft) => sft })
    ingest
  }

  override protected def getTertiaryProperties: Seq[PropertyDescriptor] =
    Seq(ExtraClasspaths, InitSchemas) ++ super.getTertiaryProperties

  /**
   * Converter ingest
   *
   * @param store data store
   * @param writers feature writers
   * @param mode schema compatibility mode
   */
  class ConverterIngest(store: DataStore, writers: FeatureWriters, mode: CompatibilityMode)
      extends IngestProcessor(store, writers, mode) {

    def init(sfts: Seq[SimpleFeatureType]): Unit = sfts.foreach(checkSchema)

    override def ingest(
        context: ProcessContext,
        session: ProcessSession,
        file: FlowFile,
        flowFileName: String): IngestResult = {
      val sft = loadFeatureType(context, file)
      checkSchema(sft)
      writers.borrow(sft.getTypeName, file) { writer =>
        val callback = new ConverterCallback() {
          override def apply(features: Iterator[SimpleFeature]): Long = {
            var failed = 0L
            features.foreach { feature =>
              try { writer.apply(feature) } catch {
                case NonFatal(e) => logError(feature, e); failed += 1
              }
            }
            failed
          }
        }
        convert(context, session, file, sft, callback)
      }
    }
  }
}

object PutGeoMesa {

  import scala.collection.JavaConverters._

  object Properties {
    val InitSchemas: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("InitializeSchemas")
          .required(false)
          .description(
            "Initialize schemas in the underlying data store when the processor is started. Schemas should be " +
                "defined in standard Java properties format, with the type name as the key, and the feature type " +
                "specification or lookup as the value")
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .addValidator(new InitSchemaValidator())
          .build()
  }

  private def initSchemas(context: PropertyContext): Seq[Either[String, SimpleFeatureType]] = {
    val string = context.getProperty(Properties.InitSchemas).evaluateAttributeExpressions().getValue
    if (string == null || string.isEmpty) { Seq.empty } else {
      val props = new java.util.Properties()
      props.load(new StringReader(string))
      props.asScala.toSeq.map { case (name, spec) =>
        SftArgResolver.getArg(SftArgs(spec, name)).left.map(e => s"$name=$spec ${e.getMessage}")
      }
    }
  }

  class InitSchemaValidator extends Validator {
    override def validate(subject: String, input: String, context: ValidationContext): ValidationResult = {
      try {
        val errors = initSchemas(context).collect { case Left(e) => e }
        if (errors.isEmpty) {
          new ValidationResult.Builder().subject(subject).input(input).valid(true).build()
        } else {
          val exp = errors.mkString("the following feature types could not be loaded: -- ", " -- ", "")
          new ValidationResult.Builder().subject(subject).input(input).explanation(exp).build()
        }
      } catch {
        case NonFatal(e) =>
          val exp = s"there was an error loading feature types: $e"
          new ValidationResult.Builder().subject(subject).input(input).explanation(exp).build()
      }
    }
  }
}
