/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.datastore.processor

import java.io.Closeable
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, ObjectPool, PooledObject}
import org.apache.nifi.annotation.lifecycle._
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.util.FormatUtils
import org.geomesa.nifi.datastore.processor.AbstractDataStoreProcessor.{PooledWriters, SingletonWriters, Writers}
import org.geotools.data._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore.SchemaCompatibility
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Abstract ingest processor for geotools data stores
  *
  * @param dataStoreProperties properties exposed through NiFi used to load the data store
  */
abstract class AbstractDataStoreProcessor(dataStoreProperties: Seq[PropertyDescriptor]) extends AbstractProcessor {

  import AbstractDataStoreProcessor.Properties._
  import Relationships.{FailureRelationship, SuccessRelationship}

  import scala.collection.JavaConverters._

  private var descriptors: Seq[PropertyDescriptor] = _
  private var relationships: Set[Relationship] = _

  @volatile
  private var ingest: IngestProcessor = _

  protected def logger: ComponentLog = getLogger

  override protected def init(context: ProcessorInitializationContext): Unit = {
    relationships = Set(SuccessRelationship, FailureRelationship)
    initDescriptors()
    logger.info(s"Props are ${descriptors.mkString(", ")}")
    logger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override def getRelationships: java.util.Set[Relationship] = relationships.asJava
  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = descriptors.asJava

  @OnAdded // reload on add to pick up any sft/converter classpath changes
  def initDescriptors(): Unit = {
    descriptors =
        getProcessorProperties ++ Seq(ExtraClasspaths) ++
            dataStoreProperties ++ getConfigProperties ++ getServiceProperties
  }

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    logger.info("Initializing")

    val dataStore = {
      val props = getDataStoreParams(context)
      lazy val safeToLog = {
        val sensitive = context.getProperties.keySet().asScala.collect { case p if p.isSensitive => p.getName }
        props.map { case (k, v) => s"$k -> ${if (sensitive.contains(k)) { "***" } else { v }}" }
      }
      logger.trace(s"DataStore properties: ${safeToLog.mkString(", ")}")
      DataStoreFinder.getDataStore(props.asJava)
    }
    require(dataStore != null, "Could not load datastore using provided parameters")

    try {
      val writers = if (context.getProperty(FeatureWriterCaching).getValue.toBoolean) {
        val timeout = context.getProperty(FeatureWriterCacheTimeout).evaluateAttributeExpressions().getValue
        new PooledWriters(dataStore, FormatUtils.getPreciseTimeDuration(timeout, TimeUnit.MILLISECONDS).toLong)
      } else {
        new SingletonWriters(dataStore)
      }
      try {
        ingest = createIngest(context, dataStore, writers)
      } catch {
        case NonFatal(e) => writers.close(); throw e
      }
    } catch {
      case NonFatal(e) => dataStore.dispose(); throw e
    }

    logger.info(s"Initialized datastore ${dataStore.getClass.getSimpleName} with ingest ${ingest.getClass.getSimpleName}")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val flowFiles = session.get(context.getProperty(NifiBatchSize).evaluateAttributeExpressions().asInteger())
    logger.debug(s"Processing ${flowFiles.size()} files in batch")
    if (flowFiles != null && flowFiles.size > 0) {
      flowFiles.asScala.foreach { f =>
        try {
          logger.debug(s"Processing file ${fullName(f)}")
          ingest.ingest(session, f)
          session.transfer(f, SuccessRelationship)
        } catch {
          case NonFatal(e) =>
            logger.error(s"Error processing file ${fullName(f)}:", e)
            session.transfer(f, FailureRelationship)
        }
      }
    }
  }

  @OnRemoved
  @OnStopped
  @OnShutdown
  def cleanup(): Unit = {
    logger.info("Processor shutting down")
    val start = System.currentTimeMillis()
    if (ingest != null) {
      CloseWithLogging(ingest)
      ingest = null
    }
    logger.info(s"Shut down in ${System.currentTimeMillis() - start}ms")
  }

  /**
   * Get the main processor params - these will come first in the UI
   *
   * @return
   */
  protected def getProcessorProperties: Seq[PropertyDescriptor] = Seq.empty

  /**
   * Get params for looking up the data store - these will come second in the UI, after the main params
   *
   * @param context context
   * @return
   */
  protected def getDataStoreParams(context: ProcessContext): Map[String, _] =
    AbstractDataStoreProcessor.getDataStoreParams(context, dataStoreProperties)

  /**
   * Get params for less common configs - these will come third in the UI, after the data store params
   *
   * @return
   */
  protected def getConfigProperties: Seq[PropertyDescriptor] =
    Seq(NifiBatchSize, FeatureWriterCaching, FeatureWriterCacheTimeout)

  /**
   * Get params for configuration services - these will come fourth in the UI, after config params
   *
   * @return
   */
  protected def getServiceProperties: Seq[PropertyDescriptor] = Seq.empty

  /**
   * Provides the processor a chance to configure a feature type before it's created in the data store
   *
   * @param sft simple feature type
   */
  protected def decorate(sft: SimpleFeatureType): SimpleFeatureType = sft

  protected def createIngest(context: ProcessContext, dataStore: DataStore, writers: Writers): IngestProcessor

  /**
   * Abstraction over ingest methods
   *
   * @param store data store
   * @param writers feature writers
   */
  abstract class IngestProcessor(store: DataStore, writers: Writers) extends Closeable {

    /**
     * Ingest a flow file
     *
     * @param session session
     * @param file flow file
     */
    def ingest(session: ProcessSession, file: FlowFile): Unit = {
      val fullFlowFileName = fullName(file)
      logger.debug(s"Running ${getClass.getName} on file $fullFlowFileName")

      val (success, failure) = ingest(session, file, fullFlowFileName)

      session.putAttribute(file, "geomesa.ingest.successes", success.toString)
      session.putAttribute(file, "geomesa.ingest.failures", failure.toString)
      logger.debug(s"Ingested file $fullFlowFileName with $success successes and $failure failures")
    }

    override def close(): Unit = store.dispose()

    /**
     * Ingest a flow file
     *
     * @param session session
     * @param file flow file
     * @param fileName file name
     * @return (success count, failure count)
     */
    protected def ingest(session: ProcessSession, file: FlowFile, fileName: String): (Long, Long)


    /**
     * Check and update the schema in the data store, as needed
     *
     * @param sft simple feature type
     */
    protected def checkSchema(sft: SimpleFeatureType): Unit = {
      store match {
        case gm: GeoMesaDataStore[_] =>
          gm.checkSchemaCompatibility(sft.getTypeName, sft) match {
            case SchemaCompatibility.Unchanged => // no-op

            case c: SchemaCompatibility.DoesNotExist =>
              logger.info(s"Creating schema ${sft.getTypeName}: ${SimpleFeatureTypes.encodeType(sft)}")
              c.apply() // create the schema

            case c: SchemaCompatibility.Compatible =>
              logger.info(
                s"Updating schema ${sft.getTypeName}:" +
                    s"\n  from ${SimpleFeatureTypes.encodeType(store.getSchema(sft.getTypeName))} " +
                    s"\n  to ${SimpleFeatureTypes.encodeType(sft)}")
              c.apply() // update the schema
              writers.invalidate(sft.getTypeName) // invalidate the writer cache

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

object AbstractDataStoreProcessor {

  import scala.collection.JavaConverters._

  def getDataStoreParams(context: ProcessContext, props: Seq[PropertyDescriptor]): Map[String, _] = {
    val builder = Map.newBuilder[String, AnyRef]
    props.foreach { p =>
      val property = {
        val prop = context.getProperty(p.getName)
        if (p.isExpressionLanguageSupported) { prop.evaluateAttributeExpressions() }  else { prop }
      }
      val value = property.getValue
      if (value != null) {
        builder += p.getName -> value
      }
    }
    builder.result
  }

  /**
   * Abstraction over feature writers
   */
  sealed trait Writers extends Closeable {

    /**
     * Get a feature writer for the given file
     *
     * @param typeName simple feature type name
     * @return
     */
    def borrowWriter(typeName: String): SimpleFeatureWriter

    /**
     *
     */
    def returnWriter(writer: SimpleFeatureWriter): Unit

    /**
     * Invalidate any cached writers
     *
     * @param typeName simple feature type name
     */
    def invalidate(typeName: String): Unit
  }

  /**
   * Pooled feature writers, re-used between flow files
   *
   * @param ds datastore
   * @param timeout how long to wait between flushes of cached feature writers, in millis
   */
  class PooledWriters(ds: DataStore, timeout: Long) extends Writers {

    private val cache = Caffeine.newBuilder().build(
      new CacheLoader[String, ObjectPool[SimpleFeatureWriter]] {
        override def load(key: String): ObjectPool[SimpleFeatureWriter] = {
          val factory = new BasePooledObjectFactory[SimpleFeatureWriter] {
            override def create(): SimpleFeatureWriter = ds.getFeatureWriterAppend(key, Transaction.AUTO_COMMIT)
            override def wrap(obj: SimpleFeatureWriter): PooledObject[SimpleFeatureWriter] = new DefaultPooledObject(obj)
            override def destroyObject(p: PooledObject[SimpleFeatureWriter]): Unit = CloseWithLogging(p.getObject)
          }
          val config = new GenericObjectPoolConfig[SimpleFeatureWriter]()
          config.setMaxTotal(-1)
          config.setMaxIdle(-1)
          config.setMinIdle(0)
          config.setMinEvictableIdleTimeMillis(timeout)
          config.setTimeBetweenEvictionRunsMillis(math.max(1000, timeout / 5))
          config.setNumTestsPerEvictionRun(10)

          new GenericObjectPool(factory, config)
        }
      }
    )

    override def borrowWriter(typeName: String): SimpleFeatureWriter = cache.get(typeName).borrowObject()

    override def returnWriter(writer: SimpleFeatureWriter): Unit =
      cache.get(writer.getFeatureType.getTypeName).returnObject(writer)

    override def invalidate(typeName: String): Unit = {
      val cur = cache.get(typeName)
      cache.invalidate(typeName)
      CloseWithLogging(cur)
    }

    override def close(): Unit = {
      CloseWithLogging(cache.asMap().values().asScala)
      ds.dispose()
    }
  }

  /**
   * Each flow file gets a new feature writer, which is closed after use
   *
   * @param ds datastore
   */
  class SingletonWriters(ds: DataStore) extends Writers {
    override def borrowWriter(typeName: String): SimpleFeatureWriter =
      ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
    override def returnWriter(writer: SimpleFeatureWriter): Unit = CloseWithLogging(writer)
    override def invalidate(typeName: String): Unit = {}
    override def close(): Unit = ds.dispose()
  }


  /**
   * Processor configuration properties
   */
  object Properties {

    val ExtraClasspaths: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("ExtraClasspaths")
          .required(false)
          .description("Add additional resources to the classpath")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .dynamicallyModifiesClasspath(true)
          .build()

    val NifiBatchSize: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("BatchSize")
          .required(false)
          .description("Number of FlowFiles to execute in a single batch")
          .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .defaultValue("5")
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
  }
}
