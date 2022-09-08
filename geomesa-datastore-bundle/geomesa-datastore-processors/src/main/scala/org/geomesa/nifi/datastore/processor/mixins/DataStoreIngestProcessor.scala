/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.datastore.processor
package mixins

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.nifi.annotation.lifecycle._
import org.apache.nifi.components.{PropertyDescriptor, PropertyValue}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.util.FormatUtils
import org.geomesa.nifi.datastore.processor.CompatibilityMode.CompatibilityMode
import org.geomesa.nifi.datastore.processor.mixins.DataStoreIngestProcessor.DynamicWriters
import org.geomesa.nifi.datastore.processor.mixins.FeatureWriters.WriteMode.{Append, Modify}
import org.geomesa.nifi.datastore.processor.mixins.FeatureWriters.{ModifyWriters, PooledWriters, SimpleWriter, SingletonWriters}
import org.geomesa.nifi.datastore.processor.validators.WriteModeValidator
import org.geotools.data._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore.SchemaCompatibility
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.Closeable
import java.util.Collections
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Abstract ingest processor for geotools data stores
  */
trait DataStoreIngestProcessor extends DataStoreProcessor {

  import DataStoreIngestProcessor.Properties._
  import Properties.NifiBatchSize

  @volatile
  private var ingest: IngestProcessor = _

  @volatile
  private var stores: Seq[DataStore] = _

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    logger.info("Initializing")

    val threads = math.max(context.getMaxConcurrentTasks, 1)
    val store = loadDataStore(context)
    stores = if (store.getClass.getSimpleName.equals("JDBCDataStore")) {
      // JDBCDataStore has synchronized blocks in the write method that prevent multi-threading,
      // the synchronization is to allow for generated fids from the database.
      // generally, this shouldn't be an issue since we use provided fids,
      // but running with 1 thread would restore the old behavior
      Seq.fill[DataStore](threads - 1)(loadDataStore(context)) :+ store
    } else {
      Seq.fill[DataStore](threads)(store)
    }

    try {
      val schemaCompatibility = CompatibilityMode.withName(context.getProperty(SchemaCompatibilityMode).getValue)

      val writers = {
        val mode = context.getProperty(WriteMode)
        val attr = context.getProperty(ModifyAttribute)
        val cacheTimeout = Option(context.getProperty(FeatureWriterCaching).getValue.toBoolean).collect {
          case true =>
            val timeout = context.getProperty(FeatureWriterCacheTimeout).evaluateAttributeExpressions().getValue
            FormatUtils.getPreciseTimeDuration(timeout, TimeUnit.MILLISECONDS).toLong
        }
        lazy val modify = {
          val value = mode.evaluateAttributeExpressions(Collections.emptyMap[String, String]).getValue
          DataStoreIngestProcessor.ModifyMode.equalsIgnoreCase(value)
        }
        if (mode.isExpressionLanguagePresent || (modify && attr.isExpressionLanguagePresent)) {
          new DynamicWriters(stores, mode, attr, cacheTimeout)
        } else {
          val writeMode =
            if (modify) {
              Modify(Option(attr.evaluateAttributeExpressions(Collections.emptyMap[String, String]).getValue))
            } else {
              Append
            }
          FeatureWriters(stores, writeMode, cacheTimeout)
        }
      }

      try {
        ingest = createIngest(context, store, writers, schemaCompatibility)
      } catch {
        case NonFatal(e) => writers.close(); throw e
      }
    } catch {
      case NonFatal(e) => stores.foreach(disposeDataStore(_, Option(context))); throw e
    }

    logger.info(s"Initialized datastore ${store.getClass.getSimpleName} with ingest ${ingest.getClass.getSimpleName}")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val file = session.get()
    if (file == null) {
      return
    }
    val name = fullName(file)
    try {
      logger.debug(s"Processing ${ingest.getClass.getName} with file $name")
      val result = ingest.ingest(context, session, file, name)
      var output = file
      output = session.putAttribute(output, Attributes.IngestSuccessCount, result.success.toString)
      output = session.putAttribute(output, Attributes.IngestFailureCount, result.failure.toString)
      logger.debug(
        s"Ingested file ${fullName(output)} with ${result.success} successes and ${result.failure} failures")
      if (result.success > 0L) {
        session.transfer(output, Relationships.SuccessRelationship)
      } else {
        session.transfer(output, Relationships.FailureRelationship)
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error processing file ${fullName(file)}:", e)
        session.transfer(file, Relationships.FailureRelationship)
    }
  }

  @OnRemoved
  @OnStopped
  def cleanup(context: ProcessContext): Unit = cleanup(Option(context))

  // note: this annotation says it should accept a ProcessContext but the test framework does not work with that
  @OnShutdown
  def cleanup(): Unit = cleanup(None)

  private def cleanup(context: Option[ProcessContext]): Unit = {
    logger.info("Processor shutting down")
    val start = System.currentTimeMillis()
    if (ingest != null) {
      CloseWithLogging(ingest)
      ingest = null
    }
    if (stores != null) {
      stores.foreach(disposeDataStore(_, context))
      stores = null
    }
    logger.info(s"Shut down in ${System.currentTimeMillis() - start}ms")
  }

  override protected def getTertiaryProperties: Seq[PropertyDescriptor] =
    super.getTertiaryProperties ++ Seq(SchemaCompatibilityMode, WriteMode, ModifyAttribute)

  override protected def getConfigProperties: Seq[PropertyDescriptor] =
    super.getConfigProperties ++ Seq(FeatureWriterCaching, FeatureWriterCacheTimeout, NifiBatchSize)

  /**
   * Log an error from writing a given feature
   *
   * @param sf feature
   * @param e error
   */
  protected def logError(sf: SimpleFeature, e: Throwable): Unit =
    logger.error(s"Error writing feature to store: '${DataUtilities.encodeFeature(sf)}'", e)

  protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: FeatureWriters,
      mode: CompatibilityMode): IngestProcessor

  /**
   * Abstraction over ingest methods
   *
   * @param store data store
   * @param writers feature writers
   */
  abstract class IngestProcessor(store: DataStore, writers: FeatureWriters, mode: CompatibilityMode)
      extends Closeable {

    private val schemaCheckCache =
      Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).build[SimpleFeatureType, Try[Unit]](
        new CacheLoader[SimpleFeatureType, Try[Unit]]() {
          override def load(sft: SimpleFeatureType): Try[Unit] = Try(doCheckSchema(sft))
        }
      )

    /**
     * Ingest a flow file
     *
     * @param context context
     * @param session session
     * @param file flow file
     * @param fileName file name
     * @return (success count, failure count)
     */
    def ingest(
        context: ProcessContext,
        session: ProcessSession,
        file: FlowFile,
        fileName: String): IngestResult

    override def close(): Unit = store.dispose()

    /**
     * Check and update the schema in the data store, as needed.
     *
     * Implementation note: the feature type is used as a key for a cache, which tracks the check against
     * the data store so we don't perform it repeatedly. This is safe to do since all our processors cache the
     * feature type object, meaning the same cache entry will be returned for each flow file that targets a
     * given type name.
     *
     * @param sft simple feature type
     */
    protected def checkSchema(sft: SimpleFeatureType): Unit = schemaCheckCache.get(sft).get // throw any error

    private def doCheckSchema(sft: SimpleFeatureType): Unit = {
      store match {
        case gm: GeoMesaDataStore[_] =>
          gm.checkSchemaCompatibility(sft.getTypeName, sft) match {
            case SchemaCompatibility.Unchanged => // no-op

            case c: SchemaCompatibility.DoesNotExist =>
              logger.info(s"Creating schema ${sft.getTypeName}: ${SimpleFeatureTypes.encodeType(sft)}")
              c.apply() // create the schema

            case c: SchemaCompatibility.Compatible =>
              mode match {
                case CompatibilityMode.Exact =>
                  logger.error(
                    s"Detected schema change ${sft.getTypeName}:" +
                        s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                        s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")
                  throw new RuntimeException(
                    s"Detected schema change for ${sft.getTypeName} but compatibility mode is set to 'exact'")

                case CompatibilityMode.Existing =>
                  logger.warn(
                    s"Detected schema change ${sft.getTypeName}:" +
                        s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                        s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")

                case CompatibilityMode.Update =>
                  logger.info(
                    s"Updating schema ${sft.getTypeName}:" +
                        s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                        s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")
                  c.apply() // update the schema
                  writers.invalidate(sft.getTypeName) // invalidate the writer cache
              }

            case c: SchemaCompatibility.Incompatible =>
              logger.error(s"Incompatible schema change detected for schema ${sft.getTypeName}")
              c.apply() // re-throw the error
          }

        case _ =>
          Try(store.getSchema(sft.getTypeName)).filter(_ != null) match {
            case Failure(_) =>
              logger.info(s"Creating schema ${sft.getTypeName}: ${SimpleFeatureTypes.encodeType(sft)}")
              store.createSchema(sft)

            case Success(existing) =>
              if (SimpleFeatureTypes.compare(existing, sft) != 0) {
                mode match {
                  case CompatibilityMode.Exact =>
                    logger.error(
                      s"Detected schema change ${sft.getTypeName}:" +
                          s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                          s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")
                    throw new RuntimeException(
                      s"Detected schema change for ${sft.getTypeName} but compatibility mode is set to 'exact'")

                  case CompatibilityMode.Existing =>
                    logger.warn(
                      s"Detected schema change ${sft.getTypeName}:" +
                          s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                          s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")

                  case CompatibilityMode.Update =>
                    logger.info(
                      s"Updating schema ${sft.getTypeName}:" +
                          s"\n  from ${SimpleFeatureTypes.encodeType(existing)} " +
                          s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")
                    store.updateSchema(sft.getTypeName, sft)
                    writers.invalidate(sft.getTypeName)
                }
              }
          }
      }
    }
  }
}

object DataStoreIngestProcessor {

  import scala.collection.JavaConverters._

  val AppendMode = "append"
  val ModifyMode = "modify"

  /**
   * Dynamically creates append or modify writers based on flow file attributes
   *
   * @param stores data stores
   * @param mode write mode property value
   * @param attribute identifying attribute property value
   * @param caching timeout for caching in millis
   */
  class DynamicWriters(stores: Seq[DataStore], mode: PropertyValue, attribute: PropertyValue, caching: Option[Long])
      extends FeatureWriters {

    private val queue = new LinkedBlockingQueue[DataStore](stores.asJava)

    private val appender = caching match {
      case None => new SingletonWriters(queue)
      case Some(timeout) => new PooledWriters(queue, timeout)
    }

    private val modifiers = Caffeine.newBuilder().build[Option[String], FeatureWriters](
      new CacheLoader[Option[String], FeatureWriters] {
        override def load(k: Option[String]): FeatureWriters = new ModifyWriters(queue, k, caching)
      }
    )

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      mode.evaluateAttributeExpressions(file).getValue match {
        case m if m == null || m.isEmpty || m.equalsIgnoreCase(AppendMode) =>
          appender.borrow(typeName, file)(fn)

        case m if m.equalsIgnoreCase(ModifyMode) =>
          val attr = Option(attribute.evaluateAttributeExpressions(file).getValue)
          modifiers.get(attr).borrow(typeName, file)(fn)

        case m =>
          throw new IllegalArgumentException(s"Invalid value for ${Properties.WriteMode.getName}: $m")
      }
    }

    override def invalidate(typeName: String): Unit = {
      appender.invalidate(typeName)
      modifiers.asMap().asScala.values.foreach(_.invalidate(typeName))
    }

    override def close(): Unit = {
      CloseWithLogging(appender)
      CloseWithLogging(modifiers.asMap().asScala.values)
    }
  }

  /**
   * Processor configuration properties
   */
  object Properties {

    val WriteMode: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("write-mode")
          .displayName("Write Mode")
          .description("Use an appending writer (for new features) or a modifying writer (to update existing features)")
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .addValidator(WriteModeValidator)
          .defaultValue(AppendMode)
          .build()

    val ModifyAttribute: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("identifying-attribute")
          .displayName("Identifying Attribute")
          .description(
            "When using a modifying writer, the attribute used to uniquely identify the feature. " +
              "If not specified, will use the feature ID")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .build()

    val FeatureWriterCaching: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("FeatureWriterCaching")
          .required(false)
          .description("Enable reuse of feature writers between flow files")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .allowableValues("true", "false")
          .defaultValue("false")
          .build()

    val FeatureWriterCacheTimeout: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("FeatureWriterCacheTimeout")
          .required(false)
          .description("How often cached feature writers will flushed to the data store")
          .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .defaultValue("5 minutes")
          .build()

    val SchemaCompatibilityMode: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("schema-compatibility")
          .displayName("Schema Compatibility")
          .required(false)
          .description(
            s"Defines how schema changes will be handled. '${CompatibilityMode.Existing}' will use " +
                s"the existing schema and drop any additional fields. '${CompatibilityMode.Update}' will " +
                s"update the existing schema to match the input schema. '${CompatibilityMode.Exact}' requires " +
                "the input schema to match the existing schema.")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .allowableValues(CompatibilityMode.Existing.toString, CompatibilityMode.Update.toString, CompatibilityMode.Exact.toString)
          .defaultValue(CompatibilityMode.Existing.toString)
          .build()
  }
}
