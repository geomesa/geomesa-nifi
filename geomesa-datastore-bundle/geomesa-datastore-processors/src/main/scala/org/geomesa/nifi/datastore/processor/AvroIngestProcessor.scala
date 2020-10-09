/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.datastore.processor

import java.io.InputStream

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.AbstractDataStoreProcessor.Writers
import org.geotools.data._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.Converters
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

object AvroIngestProcessor {

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

  def buildWriter(
                   inputSFT: SimpleFeatureType,
                   outputSFT: SimpleFeatureType,
                   matchMode: String,
                   useProvidedFid: Boolean): (SimpleFeatureWriter, SimpleFeature) => Unit = {
    FeatureTypeProcessor.checkCompatibleSchema(inputSFT, outputSFT) match {
      case None =>
        // schemas match, so use a regular writer
        (fw: SimpleFeatureWriter, sf: SimpleFeature) =>
          FeatureUtils.write(fw, sf, useProvidedFid = useProvidedFid)

      case Some(_) if matchMode == LenientMatch =>
        val sfConverter = convert(inputSFT, outputSFT)
        (fw: SimpleFeatureWriter, sf: SimpleFeature) =>
          FeatureUtils.write(fw, sfConverter(sf), useProvidedFid = useProvidedFid)

      case Some(error) => throw error
    }
  }

  import scala.collection.JavaConverters._
  // Creates an adapter from one SFT to another
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
    sf: SimpleFeature => SimpleFeatureBuilder.build(out, inToOut.map(_(sf)).asJava, sf.getID)
  }
}

/**
  * Avro ingest processor for geotools data stores
  */
trait AvroIngestProcessor extends FeatureTypeProcessor {

  import AvroIngestProcessor.{AvroMatchMode, UseProvidedFid, buildWriter}

  override protected def getProcessorProperties: Seq[PropertyDescriptor] =
    super.getProcessorProperties ++ Seq(AvroMatchMode, UseProvidedFid)

  override protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: Writers,
      sftArg: Option[String],
      typeName: Option[String]): IngestProcessor = {
    val matchMode = context.getProperty(AvroMatchMode).getValue
    val useProvidedFid = context.getProperty(UseProvidedFid).getValue.toBoolean
    new AvroIngest(dataStore, writers, sftArg, typeName, matchMode, useProvidedFid)
  }

  /**
   * GeoAvro ingest
   *
   * @param store data store
   * @param writers feature writers
   * @param spec simple feature spec
   * @param name simple feature name override
   * @param matchMode field match mode
   * @param useProvidedFid use provided fid
   */
  class AvroIngest(
      store: DataStore,
      writers: Writers,
      spec: Option[String],
      name: Option[String],
      matchMode: String,
      useProvidedFid: Boolean
    ) extends IngestProcessorWithSchema(store, writers, spec, name) {

    override protected def ingest(
        session: ProcessSession,
        file: FlowFile,
        name: String,
        sft: SimpleFeatureType,
        fw: SimpleFeatureWriter): (Long, Long) = {

      var success = 0L
      var failure = 0L

      session.read(file, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          val reader = new AvroDataFileReader(in)
          try {
            val writer = buildWriter(reader.getSft, sft, matchMode, useProvidedFid)
            reader.foreach { sf =>
              try {
                writer(fw, sf)
                success += 1L
              } catch {
                case NonFatal(e) =>
                  failure += 1L
                  logError(sf, e)
              }
            }
          } finally {
            reader.close()
          }
        }
      })

      (success, failure)
    }
  }
}

