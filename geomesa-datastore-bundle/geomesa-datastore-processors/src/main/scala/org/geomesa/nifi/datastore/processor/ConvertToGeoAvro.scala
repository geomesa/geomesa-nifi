/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import java.io.{InputStream, OutputStream}

import org.apache.nifi.annotation.behavior.WritesAttribute
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigLoader, ConverterConfigResolver}
import org.locationtech.geomesa.convert2
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.avro.AvroDataFileWriter
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs, SimpleFeatureTypeLoader}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

@Tags(Array("OGC", "geo", "convert", "converter", "simple feature", "geotools", "geomesa"))
@CapabilityDescription("Convert incoming files into OGC SimpleFeature avro data files using GeoMesa Converters")
@WritesAttribute(attribute = "mime.type", description = "the mime type of the outgoing format")
class ConvertToGeoAvro extends AbstractProcessor {

  import ConvertToGeoAvro._

  private var descriptors: java.util.List[PropertyDescriptor] = _
  private var relationships: java.util.Set[Relationship] = _

  @volatile
  private var converter: convert2.SimpleFeatureConverter = _

  protected override def init(context: ProcessorInitializationContext): Unit = {
    descriptors = List(
      SftName,
      ConverterName,
      FeatureNameOverride,
      SftSpec,
      ConverterSpec,
      OutputFormat).asJava
    relationships = Set(SuccessRelationship, FailureRelationship).asJava
  }

  override def getRelationships: java.util.Set[Relationship] = relationships
  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = descriptors

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    val sft = getSft(context)
    converter = getConverter(sft, context)
    getLogger.info(s"Initialized SimpleFeatureConverter sft and converter for type ${sft.getTypeName}")
  }

  @OnRemoved
  def cleanup(): Unit = {
    if (converter != null) {
      converter.close()
      converter = null
    }
    getLogger.info(s"Shut down ${getClass.getName} processor $getIdentifier")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
    Option(session.get()).foreach(doWork(context, session, _))

  private def doWork(context: ProcessContext, session: ProcessSession, flowFile: FlowFile): Unit = {
    try {
      val newFlowFile = session.write(flowFile, new StreamCallback {
        override def process(in: InputStream, out: OutputStream): Unit = {
          val dfw = new AvroDataFileWriter(out, converter.targetSft)
          try {
            val fullFlowFileName = flowFile.getAttribute("path") + flowFile.getAttribute("filename")
            getLogger.info(s"Converting path $fullFlowFileName")
            val ec = converter.createEvaluationContext(Map("inputFilePath" -> fullFlowFileName))
            converter.process(in, ec).foreach(dfw.append)
          } finally {
            dfw.close()
          }
        }
      })
      session.transfer(newFlowFile, SuccessRelationship)
    } catch {
      case e: Exception =>
        getLogger.error(s"Error converter file to avro: ${e.getMessage}", e)
        session.transfer(flowFile, FailureRelationship)
    }
  }

  protected def getSft(context: ProcessContext): SimpleFeatureType = {
    val sftArg = Option(context.getProperty(SftName).getValue)
      .orElse(Option(context.getProperty(SftSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${SftName.getName} or ${SftSpec.getName} property"))
    val typeName = context.getProperty(FeatureNameOverride).getValue
    SftArgResolver.getArg(SftArgs(sftArg, typeName)) match {
      case Left(e) => throw e
      case Right(sftype) => sftype
    }
  }

  protected def getConverter(sft: SimpleFeatureType, context: ProcessContext): convert2.SimpleFeatureConverter = {
    val convertArg = Option(context.getProperty(ConverterName).getValue)
      .orElse(Option(context.getProperty(ConverterSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${ConverterName.getName} or ${ConverterSpec.getName} property"))
    val config = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
      case Left(e) => throw e
      case Right(conf) => conf
    }
    SimpleFeatureConverter(sft, config)
  }
}

object ConvertToGeoAvro {

  val SftName: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("SftName")
    .description("Choose an SFT defined by a GeoMesa SFT Provider (preferred)")
    .required(false)
    .allowableValues(SimpleFeatureTypeLoader.listTypeNames.toArray: _*)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val ConverterName: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("ConverterName")
    .description("Choose an SimpleFeature Converter defined by a GeoMesa SFT Provider (preferred)")
    .required(false)
    .allowableValues(ConverterConfigLoader.listConverterNames.toArray: _*)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)   // TODO validate
    .build

  val FeatureNameOverride: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("FeatureNameOverride")
    .description("Override the Simple Feature Type name from the SFT Spec")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)  // TODO validate
    .build

  val SftSpec: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("SftSpec")
    .description("Manually define a SimpleFeatureType (SFT) config spec")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val ConverterSpec: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("ConverterSpec")
    .description("Manually define a converter using typesafe config")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val OutputFormat: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("OutputFormat")
    .description("File format for the outgoing simple feature file")
    .required(true)
    .allowableValues(Set("avro").asJava)
    .defaultValue("avro")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
}

