/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.datastore.processor

import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.ControllerService
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.geomesa.nifi.datastore.processor.CompatibilityMode.CompatibilityMode
import org.geomesa.nifi.datastore.processor.mixins.ConvertInputProcessor.ConverterCallback
import org.geomesa.nifi.datastore.processor.mixins.DataStoreIngestProcessor.FeatureWriters
import org.geomesa.nifi.datastore.processor.mixins.DataStoreIngestProcessor.FeatureWriters.SimpleWriter
import org.geomesa.nifi.datastore.processor.mixins.{ConvertInputProcessor, FeatureTypeIngestProcessor}
import org.geotools.data._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

/**
  * Converter ingest processor for geotools data stores
  */
@CapabilityDescription("Convert and ingest data files into GeoMesa")
trait ConverterIngestProcessor extends FeatureTypeIngestProcessor with ConvertInputProcessor {

  override protected def createIngest(
      ds: DataStore,
      writers: FeatureWriters,
      mode: CompatibilityMode,
      properties: Map[PropertyDescriptor, String],
      services: Map[PropertyDescriptor, ControllerService]): IngestProcessor = {
    new ConverterIngest(ds, writers, mode, properties)
  }

  /**
   * Converter ingest
   *
   * @param store data store
   * @param writers feature writers
   * @param mode schema compatibility mode
   */
  class ConverterIngest(
      store: DataStore,
      writers: FeatureWriters,
      mode: CompatibilityMode,
      properties: Map[PropertyDescriptor, String]
    ) extends IngestProcessorWithSchema(store, writers, mode, properties) {

    override protected def ingest(
        session: ProcessSession,
        file: FlowFile,
        name: String,
        sft: SimpleFeatureType,
        writer: SimpleWriter): IngestResult = {
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
      convert(session, file, sft, callback, properties)
    }
  }
}
