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
import org.apache.nifi.annotation.behavior.{ReadsAttribute, ReadsAttributes}
import org.apache.nifi.annotation.lifecycle._
import org.apache.nifi.components.{PropertyDescriptor, ValidationResult}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.util.FormatUtils
import org.geomesa.nifi.datastore.processor.validators.SimpleFeatureTypeValidator
import org.geotools.data._
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

/**
  * Abstract ingest processor for geotools data stores
  *
  * @param dataStoreProperties properties exposed through NiFi used to load the data store
  */
@ReadsAttributes(
  Array(
    new ReadsAttribute(attribute = "geomesa.sft.name", description = "GeoMesa SimpleFeatureType name"),
    new ReadsAttribute(attribute = "geomesa.sft.spec", description = "GeoMesa SimpleFeatureType specification")
  )
)
abstract class AbstractGeoIngestProcessor(dataStoreProperties: Seq[PropertyDescriptor]) extends AbstractProcessor {

  import AbstractGeoIngestProcessor.Properties._
  import AbstractGeoIngestProcessor.Relationships._
  import AbstractGeoIngestProcessor._

  import scala.collection.JavaConverters._

  private var sftName: PropertyDescriptor = _

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
    sftName = Properties.sftName(SimpleFeatureTypeLoader.listTypeNames)
    val sftProps = Seq(sftName, SftSpec, FeatureNameOverride)
    descriptors =
        sftProps ++ getProcessorProperties ++ Seq(ExtraClasspaths) ++
            dataStoreProperties ++ getConfigProperties ++ getServiceProperties
  }

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    logger.info("Initializing")

    val sftArg = AbstractGeoIngestProcessor.getFirst(context, Seq(sftName, SftSpec))
    val typeName = Option(context.getProperty(FeatureNameOverride).evaluateAttributeExpressions().getValue)

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
        new PooledWriters(dataStore, FormatUtils.getTimeDuration(timeout, TimeUnit.MILLISECONDS))
      } else {
        new SingletonWriters(dataStore)
      }
      try {
        ingest = createIngest(context, dataStore, writers, sftArg, typeName)
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
          logger.debug(s"Processing file ${AbstractGeoIngestProcessor.fullName(f)}")
          ingest.ingest(session, f)
          session.transfer(f, SuccessRelationship)
        } catch {
          case NonFatal(e) =>
            logger.error(s"Error processing file ${AbstractGeoIngestProcessor.fullName(f)}:", e)
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

  protected def getProcessorProperties: Seq[PropertyDescriptor] = Seq.empty

  protected def getConfigProperties: Seq[PropertyDescriptor] =
    Seq(NifiBatchSize, FeatureWriterCaching, FeatureWriterCacheTimeout)

  protected def getServiceProperties: Seq[PropertyDescriptor] = Seq.empty

  /**
    * Get params for looking up the data store
    *
    * @param context context
    * @return
    */
  protected def getDataStoreParams(context: ProcessContext): Map[String, _] =
    AbstractGeoIngestProcessor.getDataStoreParams(context, dataStoreProperties)

  protected def decorate(sft: SimpleFeatureType): Unit = {}

  protected def createIngest(
      context: ProcessContext,
      dataStore: DataStore,
      writers: Writers,
      sftArg: Option[String],
      typeName: Option[String]): IngestProcessor

  /**
   * Abstraction over ingest methods
   *
   * @param store data store
   * @param writers feature writers
   * @param spec simple feature spec
   * @param name simple feature name override
   */
  abstract class IngestProcessor(
      store: DataStore,
      writers: Writers,
      spec: Option[String],
      name: Option[String]
    ) extends Closeable {

    private val existingSfts = store.getTypeNames

    private val sftCache = Caffeine.newBuilder().build(
      new CacheLoader[SftArgs, Either[Throwable, SimpleFeatureType]]() {
        override def load(key: SftArgs): Either[Throwable, SimpleFeatureType] = {
          SftArgResolver.getArg(key).right.flatMap { sft =>
            try {
              decorate(sft)
              if (existingSfts.contains(sft.getTypeName)) {
                AbstractGeoIngestProcessor.checkCompatibleSchema(store.getSchema(sft.getTypeName), sft)
              } else {
                logger.info(s"Creating schema '${sft.getTypeName}'. Existing types are: ${existingSfts.mkString(", ")}")
                store.createSchema(sft)
              }
              Right(sft)
            } catch {
              case NonFatal(e) => Left(e)
            }
          }
        }
      }
    )

    /**
     * Ingest a flow file
     *
     * @param session session
     * @param file flow file
     */
    def ingest(session: ProcessSession, file: FlowFile): Unit = {
      val fullFlowFileName = fullName(file)
      logger.debug(s"Running ${getClass.getName} on file $fullFlowFileName")

      val sftSpec = Option(file.getAttribute(Attributes.SftSpecAttribute)).orElse(spec).getOrElse {
        throw new IllegalArgumentException(
          s"SimpleFeatureType not specified: configure '$SftNameKey', 'SftSpec' " +
              s"or flow-file attribute '${Attributes.SftSpecAttribute}'")
      }
      val sftName = Option(file.getAttribute(Attributes.SftNameAttribute)).orElse(name).orNull

      val sft = sftCache.get(SftArgs(sftSpec, sftName)) match {
        case Left(e) => throw e
        case Right(s) => s
      }

      val writer = writers.borrowWriter(sft.getTypeName)
      val (success, failure) = try {
        ingest(session, file, fullFlowFileName, sft, writer)
      } finally {
        writers.returnWriter(writer)
      }

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
     * @param name file name
     * @param fw feature writer
     * @return (success count, failure count)
     */
    protected def ingest(
        session: ProcessSession,
        file: FlowFile,
        name: String,
        sft: SimpleFeatureType,
        fw: SimpleFeatureWriter): (Long, Long)

    /**
     * Log an error from writing a given feature
     *
     * @param sf feature
     * @param e error
     */
    protected def logError(sf: SimpleFeature, e: Throwable): Unit =
      logger.error(s"Error writing feature to store: '${DataUtilities.encodeFeature(sf)}'", e)
  }
}

object AbstractGeoIngestProcessor {

  import scala.collection.JavaConverters._

  type SimpleFeatureWriter = FeatureWriter[SimpleFeatureType, SimpleFeature]

  /**
    * Create a validation result to mark a value invalid
    *
    * @param message message
    * @return
    */
  def invalid(message: String): ValidationResult = new ValidationResult.Builder().input(message).build()

  /**
    * Verifies the input type is compatible with the existing feature type in the data store
    *
    * Compatibility currently implies:
    *   1. feature type has the same or fewer number of attributes
    *   2. corresponding attributes have compatible type binding
    *
    * It does not imply:
    *   1. feature type has exact same number of attributes
    *   2. attributes have the same name (attribute number is used)
    *   3. attributes have the exact same binding
    *
    * @param existing current feature type
    * @param input input simple feature type
    */
  def checkCompatibleSchema(existing: SimpleFeatureType, input: SimpleFeatureType): Unit = {
    require(existing != null) // if we're calling this method the schema should have already been created

    lazy val exception =
      new IllegalArgumentException("Input schema does not match existing type:" +
          s"\n\tInput:    ${SimpleFeatureTypes.encodeType(input)}" +
          s"\n\tExisting: ${SimpleFeatureTypes.encodeType(existing)}")

    if (input.getAttributeCount > existing.getAttributeCount) {
      throw exception
    }

    var i = 0
    while (i < input.getAttributeCount) {
      if (!existing.getDescriptor(i).getType.getBinding.isAssignableFrom(input.getDescriptor(i).getType.getBinding)) {
        throw exception
      }
      i += 1
    }
  }

  def getFirst(context: ProcessContext, props: Seq[PropertyDescriptor]): Option[String] =
    props.toStream.flatMap(p => Option(context.getProperty(p).getValue)).headOption

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
   * Full name of a flow file
   *
   * @param f flow file
   * @return
   */
  private def fullName(f: FlowFile): String = f.getAttribute("path") + f.getAttribute("filename")

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
    override def close(): Unit = ds.dispose()
  }

  /**
    * Processor configuration properties
    */
  object Properties {

    val SftNameKey = "SftName"

    def sftName(values: Seq[String]): PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name(SftNameKey)
          .required(false)
          .description("Choose a simple feature type defined by a GeoMesa SFT Provider (preferred)")
          .allowableValues(values.sorted: _*)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build()

    val SftSpec: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("SftSpec")
          .required(false)
          .description("Manually define a SimpleFeatureType (SFT) config spec")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .addValidator(SimpleFeatureTypeValidator)
          .build()

    val FeatureNameOverride: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("FeatureNameOverride")
          .required(false)
          .description("Override the Simple Feature Type name from the SFT Spec")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .build()

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

  object Attributes {

    val ConverterAttribute = "geomesa.converter"
    val SftNameAttribute   = "geomesa.sft.name"
    val SftSpecAttribute   = "geomesa.sft.spec"

    val all: Seq[String] = Seq(ConverterAttribute, SftNameAttribute, SftSpecAttribute)
  }

  object Relationships {
    final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
    final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
  }
}
