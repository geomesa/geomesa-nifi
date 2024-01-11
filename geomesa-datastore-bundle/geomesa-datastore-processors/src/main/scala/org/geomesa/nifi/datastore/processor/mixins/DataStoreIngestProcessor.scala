/***********************************************************************
 * Copyright (c) 2015-2024 Commonwealth Computer Research, Inc.
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
import org.geomesa.nifi.datastore.processor.mixins.FeatureWriters.SimpleWriter
import org.geomesa.nifi.datastore.processor.validators.WriteModeValidator
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.data._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore.SchemaCompatibility
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.Closeable
import java.util.Collections
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Abstract ingest processor for geotools data stores
  */
trait DataStoreIngestProcessor extends DataStoreProcessor {

  import DataStoreIngestProcessor.Properties._

  @volatile
  private var ingest: IngestProcessor = _

  @volatile
  private var service: DataStoreService = _

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    logger.info("Initializing")

    service = getDataStoreService(context)

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
        new DynamicWriters(service, mode, attr, cacheTimeout)
      } else if (modify) {
        val attribute = Option(attr.evaluateAttributeExpressions(Collections.emptyMap[String, String]).getValue)
        FeatureWriters.modifier(service, attribute)
      } else {
        FeatureWriters.appender(service, cacheTimeout)
      }
    }

    try {
      ingest = createIngest(context, service, writers, schemaCompatibility)
    } catch {
      case NonFatal(e) => CloseWithLogging(writers); throw e
    }

    logger.info(
      s"Initialized DataStoreService ${service.getClass.getSimpleName} with ingest ${ingest.getClass.getSimpleName}")
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
    logger.info(s"Shut down in ${System.currentTimeMillis() - start}ms")
  }

  override protected def getTertiaryProperties: Seq[PropertyDescriptor] =
    super.getTertiaryProperties ++ Seq(SchemaCompatibilityMode, WriteMode, ModifyAttribute)

  override protected def getConfigProperties: Seq[PropertyDescriptor] =
    super.getConfigProperties ++ Seq(FeatureWriterCaching, FeatureWriterCacheTimeout)

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
      service: DataStoreService,
      writers: FeatureWriters,
      mode: CompatibilityMode): IngestProcessor

  /**
   * Abstraction over ingest methods
   *
   * @param service data store service
   * @param writers feature writers
   */
  abstract class IngestProcessor(service: DataStoreService, writers: FeatureWriters, mode: CompatibilityMode)
      extends Closeable {

    private val refresher = ExitingExecutor(new ScheduledThreadPoolExecutor(1))

    private val schemaCheckCache =
      Caffeine.newBuilder().build[SimpleFeatureType, Try[Unit]](
        new CacheLoader[SimpleFeatureType, Try[Unit]]() {
          override def load(sft: SimpleFeatureType): Try[Unit] = {
            // we schedule a refresh, vs using the built-in Caffeine refresh which will only refresh after a
            // request. If there are not very many requests, then the deferred value will always be out of date
            refresher.schedule(new Runnable() { override def run(): Unit = refresh(sft) }, 1, TimeUnit.HOURS)
            Try {
              val store = service.loadDataStore()
              try { doCheckSchema(store, sft) } finally {
                service.dispose(store)
              }
            }
          }
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

    override def close(): Unit = {
      CloseWithLogging(writers)
      refresher.shutdownNow()
    }

    private def refresh(sft: SimpleFeatureType): Unit = schemaCheckCache.refresh(sft)

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

    private def doCheckSchema(store: DataStore, sft: SimpleFeatureType): Unit = {
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
   * @param service data store service
   * @param mode write mode property value
   * @param attribute identifying attribute property value
   * @param caching timeout for caching in millis
   */
  class DynamicWriters(service: DataStoreService, mode: PropertyValue, attribute: PropertyValue, caching: Option[Long])
      extends FeatureWriters {

    private val appender = FeatureWriters.appender(service, caching)

    private val modifiers = Caffeine.newBuilder().build[Option[String], FeatureWriters](
      new CacheLoader[Option[String], FeatureWriters] {
        override def load(k: Option[String]): FeatureWriters = FeatureWriters.modifier(service, k)
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
