/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.records

import java.io.InputStream

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.Record
import org.geomesa.nifi.datastore.processor.AbstractDataStoreProcessor
import org.geomesa.nifi.datastore.processor.AbstractDataStoreProcessor.Writers
import org.geomesa.nifi.datastore.processor.records.RecordIngestProcessor.CountHolder
import org.geomesa.nifi.datastore.processor.records.Properties._
import org.geotools.data._
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec.GeomAttributeSpec
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser
import org.locationtech.geomesa.utils.io.WithClose

import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
  * Record-based ingest processor for geotools data stores
  */
trait RecordIngestProcessor extends AbstractDataStoreProcessor {

  override protected def getProcessorProperties: Seq[PropertyDescriptor] =
    super.getProcessorProperties ++ RecordIngestProcessor.Props

  override protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: Writers): IngestProcessor = {
    val factory = context.getProperty(RecordReader).asControllerService(classOf[RecordReaderFactory])
    val options = {
      val typeName = Option(context.getProperty(TypeName).evaluateAttributeExpressions().getValue)
      val fidCol = Option(context.getProperty(FeatureIdCol).evaluateAttributeExpressions().getValue)
      val geomCols =
        Option(context.getProperty(GeometryCols).evaluateAttributeExpressions().getValue).toSeq.flatMap { spec =>
          SimpleFeatureSpecParser.parse(spec).attributes.collect {
            case g: GeomAttributeSpec => GeometryColumn(g.name, g.clazz, g.default)
          }
        }
      val geomEncoding = GeometryEncoding(context.getProperty(GeometrySerializationDefaultWkt).getValue)
      val jsonCols =
        Option(context.getProperty(JsonCols).evaluateAttributeExpressions().getValue).toSeq.flatMap(_.split(","))
      val dtgCol = Option(context.getProperty(DefaultDateCol).evaluateAttributeExpressions().getValue)
      val visCol = Option(context.getProperty(VisibilitiesCol).evaluateAttributeExpressions().getValue)
      val userData = Option(context.getProperty(SchemaUserData).evaluateAttributeExpressions().getValue) match {
        case Some(spec) => SimpleFeatureSpecParser.parse(";" + spec).options
        case None => Map.empty[String, AnyRef]
      }
      RecordConverterOptions(typeName, fidCol, geomCols, geomEncoding, jsonCols, dtgCol, visCol, userData)
    }

    new RecordIngest(dataStore, writers, factory, options)
  }

  /**
   * Record based ingest
   *
   * @param store data store
   * @param writers feature writers
   * @param recordReaderFactory record reader factory
   * @param options record conversion options
   */
  class RecordIngest(
      store: DataStore,
      writers: Writers,
      recordReaderFactory: RecordReaderFactory,
      options: RecordConverterOptions
    ) extends IngestProcessor(store, writers) {

    import scala.collection.JavaConverters._

    override protected def ingest(session: ProcessSession, file: FlowFile, fileName: String): (Long, Long) = {

      val counts = new CountHolder()

      session.read(file, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          WithClose(recordReaderFactory.createRecordReader(file, in, logger)) { reader =>
            val converter = SimpleFeatureRecordConverter(reader.getSchema, options)
            // create or update the feature type if needed
            checkSchema(decorate(converter.sft))

            @tailrec
            def nextRecord: Record = {
              try {
                return reader.nextRecord()
              } catch {
                case NonFatal(e) =>
                  counts.failure += 1L
                  logger.error("Error reading record from file", e)
              }
              nextRecord
            }

            val writer = writers.borrowWriter(converter.sft.getTypeName)
            try {
              var record = nextRecord
              while (record != null) {
                try {
                  val sf = converter.convert(record)
                  try {
                    FeatureUtils.write(writer, sf)
                    counts.success += 1L
                  } catch {
                    case NonFatal(e) =>
                      counts.failure += 1L
                      logger.error(s"Error writing feature to store: '${DataUtilities.encodeFeature(sf)}'", e)
                  }
                } catch {
                  case NonFatal(e) =>
                    counts.failure += 1L
                    logger.error(s"Error converting record to feature: '${record.toMap.asScala.mkString(",")}'", e)
                }
                record = nextRecord
              }
            } finally {
              writers.returnWriter(writer)
            }
          }
        }
      })

      (counts.success, counts.failure)
    }
  }
}

object RecordIngestProcessor {

  private val Props = Seq(
    RecordReader,
    TypeName,
    FeatureIdCol,
    GeometryCols,
    GeometrySerializationDefaultWkt,
    JsonCols,
    DefaultDateCol,
    VisibilitiesCol,
    SchemaUserData
  )

  class CountHolder(var success: Long = 0L, var failure: Long = 0L)
}
