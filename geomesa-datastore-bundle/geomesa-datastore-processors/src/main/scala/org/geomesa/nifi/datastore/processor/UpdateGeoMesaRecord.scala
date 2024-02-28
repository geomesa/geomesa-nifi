/***********************************************************************
 * Copyright (c) 2015-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching, WritesAttribute, WritesAttributes}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled, OnShutdown, OnStopped}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.{ProcessContext, ProcessSession}
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.Record
import org.geomesa.nifi.datastore.processor.UpdateGeoMesaRecord.{AttributeFilter, FidFilter, SchemaCache}
import org.geomesa.nifi.datastore.processor.mixins.DataStoreProcessor
import org.geomesa.nifi.datastore.processor.records.{GeometryEncoding, OptionExtractor, SimpleFeatureRecordConverter}
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.api.data.{DataStore, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data.DataUtilities
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.{Closeable, InputStream}
import java.util.concurrent.{LinkedBlockingQueue, ScheduledThreadPoolExecutor, TimeUnit}
import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


@Tags(Array("geomesa", "geo", "update", "records", "geotools"))
@CapabilityDescription("Update existing features in GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes(
  Array(
    new WritesAttribute(attribute = "geomesa.ingest.successes", description = "Number of features updated successfully"),
    new WritesAttribute(attribute = "geomesa.ingest.failures", description = "Number of features with errors")
  )
)
@SupportsBatching
class UpdateGeoMesaRecord extends DataStoreProcessor {

  import UpdateGeoMesaRecord.Properties.LookupCol
  import org.geomesa.nifi.datastore.processor.records.Properties.RecordReader
  import org.locationtech.geomesa.security.SecureSimpleFeature

  import scala.collection.JavaConverters._

  @volatile
  private var service: DataStoreService = _
  private var factory: RecordReaderFactory = _
  private var options: OptionExtractor = _
  private var schemas: SchemaCache = _

  private val stores = new LinkedBlockingQueue[DataStore]()

  override protected def getPrimaryProperties: Seq[PropertyDescriptor] =
    super.getPrimaryProperties ++ UpdateGeoMesaRecord.Props

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    logger.info("Initializing")

    service = getDataStoreService(context)
    schemas = new SchemaCache(service)
    factory = context.getProperty(RecordReader).asControllerService(classOf[RecordReaderFactory])
    options = OptionExtractor(context, GeometryEncoding.Wkt)

    logger.info(s"Initialized with DataStoreService ${service.getClass.getSimpleName}")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val file = session.get()
    if (file == null) {
      return
    }

    val fullFlowFileName = fullName(file)
    try {
      logger.debug(s"Running ${getClass.getName} on file $fullFlowFileName")

      val opts = options(context, file.getAttributes)
      val id = context.getProperty(LookupCol).evaluateAttributeExpressions(file).getValue
      val filterFactory = if (opts.fidField.contains(id)) { FidFilter } else { new AttributeFilter(id) }

      var success, failure = 0L

      session.read(file, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          WithClose(factory.createRecordReader(file, in, logger)) { reader =>
            val converter = SimpleFeatureRecordConverter(reader.getSchema, opts)
            val typeName = converter.sft.getTypeName

            val existing = schemas.getSchema(typeName)
            if (existing == null) {
              throw new IllegalStateException(s"Schema '$typeName' does not exist in the data store")
            }

            val names = converter.sft.getAttributeDescriptors.asScala.flatMap { d =>
              val name = d.getLocalName
              if (existing.indexOf(name) == -1) {
                logger.warn(s"Attribute '$name' does not exist in the schema and will be ignored")
                None
              } else {
                Some(name)
              }
            }

            @tailrec
            def nextRecord: Record = {
              try {
                return reader.nextRecord()
              } catch {
                case NonFatal(e) =>
                  failure += 1L
                  logger.error("Error reading record from file", e)
              }
              nextRecord
            }

            var ds = stores.poll() match {
              case null => service.loadDataStore()
              case ds => ds
            }

            try {
              val records = Iterator.continually(nextRecord).takeWhile(_ != null)
              val features = records.flatMap { record =>
                try {
                  val sf = converter.convert(record)
                  val filter = filterFactory(sf)
                  if (ds == null) {
                    failure += 1L
                    Iterator.empty
                  } else {
                    Iterator.single(sf -> filter)
                  }
                } catch {
                  case NonFatal(e) =>
                    failure += 1L
                    logger.error(s"Error converting record to feature: '${record.toMap.asScala.mkString(",")}'", e)
                    Iterator.empty
                }
              }

              features.foreach { case (sf, filter) =>
                Try(ds.getFeatureWriter(typeName, filter, Transaction.AUTO_COMMIT)) match {
                  case Failure(e) =>
                    // if we can't get the writer, that usually means the datastore has become invalid
                    failure += 1L
                    logger.error(s"Error getting feature writer:", e)
                    service.dispose(ds)
                    ds = null // this will filter out and fail any remaining records in our iterator, above

                  case Success(writer) =>
                    try {
                      if (!writer.hasNext) {
                        logger.warn(s"Filter does not match any features, skipping update: ${filterToString(filter)}")
                        failure += 1L
                      } else {
                        do {
                          val toWrite = writer.next()
                          names.foreach(n => toWrite.setAttribute(n, sf.getAttribute(n)))
                          if (opts.fidField.isDefined) {
                            toWrite.getUserData.put(Hints.PROVIDED_FID, sf.getID)
                          }
                          if (opts.visField.isDefined) {
                            sf.visibility.foreach(toWrite.visibility = _)
                          }
                          writer.write()
                          success += 1L
                        } while (writer.hasNext)
                      }
                    } catch {
                      case NonFatal(e) =>
                        failure += 1L
                        logger.error(s"Error writing feature to store: '${DataUtilities.encodeFeature(sf)}'", e)
                    } finally {
                      CloseWithLogging(writer)
                    }
                }
              }
            } finally {
              if (ds != null) {
                stores.put(ds)
              }
            }
          }
        }
      })

      var output = file
      output = session.putAttribute(output, Attributes.IngestSuccessCount, success.toString)
      output = session.putAttribute(output, Attributes.IngestFailureCount, failure.toString)
      session.transfer(output, Relationships.SuccessRelationship)

      logger.debug(s"Ingested file $fullFlowFileName with $success successes and $failure failures")
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error processing file $fullFlowFileName:", e)
        session.transfer(file, Relationships.FailureRelationship)
    }
  }

  @OnRemoved
  @OnStopped
  @OnShutdown
  def cleanup(): Unit = {
    logger.info("Processor shutting down")
    val start = System.currentTimeMillis()
    stores.iterator().asScala.foreach(service.dispose)
    stores.clear()
    if (schemas != null) {
      CloseWithLogging(schemas)
      schemas = null
    }
    logger.info(s"Shut down in ${System.currentTimeMillis() - start}ms")
  }
}

object UpdateGeoMesaRecord {

  import UpdateGeoMesaRecord.Properties.LookupCol
  import org.geomesa.nifi.datastore.processor.records.Properties._
  import org.locationtech.geomesa.filter.FilterHelper.ff

  private val Props = Seq(
    RecordReader,
    TypeName,
    LookupCol,
    FeatureIdCol,
    GeometryCols,
    GeometrySerializationDefaultWkt,
    VisibilitiesCol
  )

  sealed trait QueryFilter {
    def apply(f: SimpleFeature): Filter
  }

  object FidFilter extends QueryFilter {
    override def apply(f: SimpleFeature): Filter = ff.id(ff.featureId(f.getID))
  }

  class AttributeFilter(name: String) extends QueryFilter {
    private val prop = ff.property(name)
    override def apply(f: SimpleFeature): Filter = ff.equals(prop, ff.literal(f.getAttribute(name)))
  }

  private class SchemaCache(service: DataStoreService) extends Closeable {

    private val refresher = ExitingExecutor(new ScheduledThreadPoolExecutor(1))

    private val schemaCheckCache =
      Caffeine.newBuilder().build[String, SimpleFeatureType](
        new CacheLoader[String, SimpleFeatureType]() {
          override def load(typeName: String): SimpleFeatureType = {
            // we schedule a refresh, vs using the built-in Caffeine refresh which will only refresh after a
            // request. If there are not very many requests, then the deferred value will always be out of date
            refresher.schedule(new Runnable() { override def run(): Unit = refresh(typeName) }, 1, TimeUnit.HOURS)
            val store = service.loadDataStore()
            try { Try(store.getSchema(typeName)).getOrElse(null) } finally {
              service.dispose(store)
            }
          }
        }
      )

    private def refresh(typeName: String): Unit = schemaCheckCache.refresh(typeName)

    def getSchema(typeName: String): SimpleFeatureType = schemaCheckCache.get(typeName)

    override def close(): Unit = refresher.shutdownNow()
  }

  object Properties {
    val LookupCol: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("lookup-col")
          .displayName("Lookup column")
          .description("Column that will be used to match features for update")
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
          .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
          .required(true)
          .build()
  }
}

