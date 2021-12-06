/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.datastore.processor

import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.geomesa.nifi.datastore.processor.CompatibilityMode.CompatibilityMode
import org.geomesa.nifi.datastore.processor.mixins.ConvertInputProcessor.ConverterCallback
import org.geomesa.nifi.datastore.processor.mixins.{ConvertInputProcessor, DataStoreIngestProcessor, FeatureWriters}
import org.geotools.data._
import org.opengis.feature.simple.SimpleFeature

import scala.util.control.NonFatal

/**
  * Converter ingest processor for geotools data stores
  */
@CapabilityDescription("Convert and ingest data files into GeoMesa")
trait ConverterIngestProcessor extends DataStoreIngestProcessor with ConvertInputProcessor {

  override protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: FeatureWriters,
      mode: CompatibilityMode): IngestProcessor = {
    new ConverterIngest(dataStore, writers, mode)
  }

  /**
   * Converter ingest
   *
   * @param store data store
   * @param writers feature writers
   * @param mode schema compatibility mode
   */
  class ConverterIngest(store: DataStore, writers: FeatureWriters, mode: CompatibilityMode)
      extends IngestProcessor(store, writers, mode) {

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
