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
import org.geomesa.nifi.datastore.processor.AbstractGeoIngestProcessor.checkCompatibleSchema
import org.geomesa.nifi.datastore.processor.AvroIngestProcessor.{AvroMatchMode, buildWriter}
import org.geotools.data._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.Converters
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.{Failure, Success}
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

  def buildWriter(sft1: SimpleFeatureType,
                  sft2: SimpleFeatureType,
                  matchMode: String): (FeatureWriter[SimpleFeatureType, SimpleFeature], SimpleFeature) => Unit = {
    checkCompatibleSchema(sft1, sft2) match {
      case Success(()) =>
        // Schemas match, so use a regular writer.
        // TODO:  Discuss if the useProvidedFid should be true!
        (fw: FeatureWriter[SimpleFeatureType, SimpleFeature], sf: SimpleFeature) =>
          FeatureUtils.write(fw, sf)
      case Failure(error) =>
        if (matchMode == LenientMatch) {
          val sfConverter = convert(sft1, sft2)
          (fw: FeatureWriter[SimpleFeatureType, SimpleFeature], sf: SimpleFeature) => {
            val sfToWrite = sfConverter(sf)
            FeatureUtils.write(fw, sfToWrite)
          }
        } else {
          throw error
        }
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
trait AvroIngestProcessor extends AbstractGeoIngestProcessor {

  import AbstractGeoIngestProcessor._

  override protected def getProcessorProperties: Seq[PropertyDescriptor] =
    super.getProcessorProperties ++ Seq(AvroMatchMode)

  override protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: Writers,
      sftArg: Option[String],
      typeName: Option[String]): IngestProcessor = {
    val setting = context.getProperty(AvroMatchMode).getValue
    new AvroIngest(dataStore, writers, sftArg, typeName, setting)
  }

  /**
   * GeoAvro ingest
   *
   * @param store data store
   * @param writers feature writers
   * @param spec simple feature spec
   * @param name simple feature name override
   */
  class AvroIngest(
      store: DataStore,
      writers: Writers,
      spec: Option[String],
      name: Option[String],
      matchMode: String
    ) extends IngestProcessor(store, writers, spec, name) {

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
            val writer = buildWriter(sft, reader.getSft, matchMode)
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

