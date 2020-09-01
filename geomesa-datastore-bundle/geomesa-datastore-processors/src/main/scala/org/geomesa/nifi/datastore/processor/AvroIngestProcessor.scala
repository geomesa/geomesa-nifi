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
  val AvroMatchMode: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("Avro SFT match mode")
      .description("Determines how Avro SFT mismatches are handled")
      .required(false)
      .defaultValue("strict")
      .allowableValues("strict", "lenient")
      .build()

  def buildWriter(sft1: SimpleFeatureType,
                  sft2: SimpleFeatureType,
                  matchMode: String): (FeatureWriter[SimpleFeatureType, SimpleFeature], SimpleFeature) => Unit = {
    checkCompatibleSchema(sft1, sft2) match {
      case Success(true) =>
        // Schemas match, so use a regular writer.
        // TODO:  Discuss if the useProvidedFid should be true!
        (fw: FeatureWriter[SimpleFeatureType, SimpleFeature], sf: SimpleFeature) =>
          FeatureUtils.write(fw, sf, true)
      case Failure(error) =>
        if (matchMode == "lenient") {
          val sfConverter = convert(sft1, sft2)
          (fw: FeatureWriter[SimpleFeatureType, SimpleFeature], sf: SimpleFeature) => {
            val sfToWrite = sfConverter(sf)
            FeatureUtils.write(fw, sfToWrite, true)
          }
        } else {
          throw error
        }
    }
  }

  import scala.collection.JavaConverters._
  // Creates an adapter from one SFT to another
  def convert(in: SimpleFeatureType, out: SimpleFeatureType): SimpleFeature => SimpleFeature = {
    val out_geometry_ln = out.getGeometryDescriptor.getLocalName
    val in2out = out.getAttributeDescriptors.asScala.map { out_ad =>
      val out_ad_ln = out_ad.getLocalName
      in.indexOf(out_ad_ln) match {
        case in_index if in_index >= 0 =>
          val out_ad_tb = out_ad.getType.getBinding
          if (out_ad_tb.isAssignableFrom(in.getType(in_index).getBinding)) {
            sf: SimpleFeature => sf.getAttribute(in_index)
          } else {
            sf: SimpleFeature => Converters.convert(sf.getAttribute(in_index), out_ad_tb).asInstanceOf[AnyRef]
          }
        case _ if out_ad_ln.equals(out_geometry_ln) => sf: SimpleFeature => sf.getDefaultGeometry
        case _ => _: SimpleFeature => null
      }
    }
    sf: SimpleFeature => SimpleFeatureBuilder.build(out, in2out.map(_(sf)).asJava, sf.getID)
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

