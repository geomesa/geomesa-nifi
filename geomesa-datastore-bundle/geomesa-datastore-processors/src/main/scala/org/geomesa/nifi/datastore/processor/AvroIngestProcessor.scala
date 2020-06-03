/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.datastore.processor

import java.io.InputStream

import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.geotools.data._
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Avro ingest processor for geotools data stores
  */
trait AvroIngestProcessor extends AbstractGeoIngestProcessor {

  import AbstractGeoIngestProcessor._

  override protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: Writers,
      sftArg: Option[String],
      typeName: Option[String]): IngestProcessor = {
    new AvroIngest(dataStore, writers, sftArg, typeName)
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
      name: Option[String]
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
            checkCompatibleSchema(sft, reader.getSft)
            reader.foreach { sf =>
              try {
                FeatureUtils.write(fw, sf)
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

