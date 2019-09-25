/***********************************************************************
 * Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.geo

import java.io.{Closeable, InputStream}
import java.util.Collections
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled, OnShutdown, OnStopped}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.util.FormatUtils
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Relationships._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor._
import org.geomesa.nifi.geo.validators.{ConverterValidator, SimpleFeatureTypeValidator}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data._
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

/**
  * Abstract ingest processor for geotools data stores
  *
  * @param dataStoreProperties properties exposed through NiFi to configure the data store connection
  */
abstract class AbstractGeoIngestProcessor(dataStoreProperties: Seq[PropertyDescriptor]) extends AbstractProcessor {

  import scala.collection.JavaConverters._

  private var descriptors: Seq[PropertyDescriptor] = _
  private var relationships: Set[Relationship] = _

  @volatile private var ingest: Ingest = _
  @volatile private var writers: Writers = _

  protected def logger: ComponentLog = getLogger

  override protected def init(context: ProcessorInitializationContext): Unit = {
    relationships = Set(SuccessRelationship, FailureRelationship)
    descriptors = Seq(
      IngestModeProp,
      SftName,
      SftSpec,
      FeatureNameOverride,
      ConverterName,
      ConverterSpec,
      ConverterErrorMode,
      NifiBatchSize,
      FeatureWriterCaching,
      FeatureWriterCacheTimeout
    ) ++ dataStoreProperties
    logger.info(s"Props are ${descriptors.mkString(", ")}")
    logger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override def getRelationships: java.util.Set[Relationship] = relationships.asJava
  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = descriptors.asJava

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    logger.info("Initializing")

    // Data store comes first...then getSft because
    // oddly enough sometimes you want to modify the sft
    val dataStore = loadDataStore(context, Map.empty)
    require(dataStore != null, "Datastore is null")

    val sft = loadSft(context)

    val existing = dataStore.getTypeNames
    if (existing.contains(sft.getTypeName)) {
      AbstractGeoIngestProcessor.checkCompatibleSchema(dataStore.getSchema(sft.getTypeName), sft)
    } else {
      logger.info(s"Creating schema '${sft.getTypeName}'. Existing types are: ${existing.mkString(", ")}")
      dataStore.createSchema(sft)
    }

    ingest = context.getProperty(IngestModeProp).getValue match {
      case IngestMode.Converter =>
        val convertArg = AbstractGeoIngestProcessor.getFirst(context, Seq(ConverterName, ConverterSpec))
        var config = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
          case Left(e) => throw e
          case Right(conf) => conf
        }
        Option(context.getProperty(ConverterErrorMode).getValue).foreach { mode =>
          val opts = ConfigValueFactory.fromMap(Collections.singletonMap("error-mode", mode))
          config = ConfigFactory.empty().withValue("options", opts).withFallback(config)
        }
        new ConverterIngest(sft, config, logger)

      case IngestMode.AvroDataFile => new AvroIngest(sft, logger)

      case m => throw new IllegalStateException(s"Unknown ingest type: $m")
    }

    writers = if (context.getProperty(FeatureWriterCaching).getValue.toBoolean) {
      val timeout = context.getProperty(FeatureWriterCacheTimeout).getValue
      val millis = FormatUtils.getTimeDuration(timeout, TimeUnit.MILLISECONDS)
      new PooledWriters(dataStore, sft.getTypeName, millis)
    } else {
      new SingletonWriters(dataStore, sft.getTypeName)
    }

    logger.info(s"Initialized datastore ${dataStore.getClass.getSimpleName} " +
        s"with feature type ${sft.getTypeName} in mode ${ingest.getClass.getName}")
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {
    val result = new java.util.ArrayList[ValidationResult]()

    // If using converters check for params relevant to that
    if (validationContext.getProperty(IngestModeProp).getValue == IngestMode.Converter) {
      // make sure either a sft is named or written
      if (!Seq(SftName, SftSpec).exists(validationContext.getProperty(_).isSet)) {
        result.add(AbstractGeoIngestProcessor.invalid("Specify a simple feature type by name or spec"))
      }
      if (!Seq(ConverterName, ConverterSpec).exists(validationContext.getProperty(_).isSet)) {
        result.add(AbstractGeoIngestProcessor.invalid("Specify a converter by name or spec"))
      }
    }

    result
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val flowFiles = session.get(context.getProperty(NifiBatchSize).asInteger())
    logger.debug(s"Processing ${flowFiles.size()} files in batch")
    if (flowFiles != null && flowFiles.size > 0) {
      writers.apply { fw =>
        flowFiles.asScala.foreach { f =>
          lazy val fullName = AbstractGeoIngestProcessor.fullName(f)
          try {
            logger.debug(s"Processing file $fullName")
            ingest.ingest(fw, session, f)
            session.transfer(f, SuccessRelationship)
          } catch {
            case NonFatal(e) =>
              logger.error(s"Error processing file $fullName:", e)
              session.transfer(f, FailureRelationship)
          }
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
    if (writers != null) {
      CloseWithLogging(writers)
      writers = null
    }
    if (ingest != null) {
      CloseWithLogging(ingest)
      ingest = null
    }
    logger.info(s"Shut down in ${System.currentTimeMillis() - start}ms")
  }

  /**
    * Loads a simple feature type from the environment, based on the configured NiFi properties
    *
    * @param context context
    * @return
    */
  protected def loadSft(context: ProcessContext): SimpleFeatureType = {
    val sftArg = AbstractGeoIngestProcessor.getFirst(context, Seq(SftName, SftSpec))
    val typeName = context.getProperty(FeatureNameOverride).getValue
    SftArgResolver.getArg(SftArgs(sftArg, typeName)) match {
      case Left(e) => throw e
      case Right(s) => s
    }
  }

  /**
    * Load the data store based on the configured nifi properties
    *
    * @param context context
    * @param static additional static data store parameters, if any
    * @return
    */
  protected def loadDataStore(context: ProcessContext, static: Map[String, _]): DataStore = {
    val configured = dataStoreProperties.flatMap { p =>
      val value = context.getProperty(p.getName).getValue
      if (value == null) { Seq.empty } else {
        Seq(p.getName -> value)
      }
    }
    val props = configured.toMap ++ static // note: static props take precedence
    logger.trace(s"DataStore properties: ${props.map { case (k, v) => s"$k -> $v" }.mkString(", ")}")
    DataStoreFinder.getDataStore(props.asJava)
  }
}

object AbstractGeoIngestProcessor {

  type FeatureWriterSimple = FeatureWriter[SimpleFeatureType, SimpleFeature]

  /**
    * Creates a nifi property descriptor based on a geotools data store parameter
    *
    * @param param param
    * @return
    */
  def property(param: Param): PropertyDescriptor = property(param, canBeRequired = true)

  /**
    * Creates a nifi property descriptor based on a geotools data store parameter
    *
    * @param param param
    * @param canBeRequired disable any 'required' flags
    * @return
    */
  def property(param: Param, canBeRequired: Boolean): PropertyDescriptor = {
    val validator = param.getType match {
      case x if classOf[java.lang.Integer].isAssignableFrom(x) => StandardValidators.INTEGER_VALIDATOR
      case x if classOf[java.lang.Long].isAssignableFrom(x)    => StandardValidators.LONG_VALIDATOR
      case x if classOf[java.lang.Boolean].isAssignableFrom(x) => StandardValidators.BOOLEAN_VALIDATOR
      case x if classOf[java.lang.String].isAssignableFrom(x)  => StandardValidators.NON_EMPTY_VALIDATOR
      case _                                                   => StandardValidators.NON_EMPTY_VALIDATOR
    }
    val sensitive =
      Option(param.metadata.get(Parameter.IS_PASSWORD).asInstanceOf[java.lang.Boolean]).exists(_.booleanValue)

    val builder =
      new PropertyDescriptor.Builder()
        .name(param.getName)
        .description(param.getDescription.toString)
        .defaultValue(Option(param.getDefaultValue).map(_.toString).orNull)
        .required(canBeRequired && param.required)
        .addValidator(validator)
        .sensitive(sensitive)

    if (classOf[java.lang.Boolean].isAssignableFrom(param.getType)) {
      builder.allowableValues("true", "false")
    }

    builder.build()
  }

  /**
    * Create a validation result to mark a value invalid
    *
    * @param message message
    * @return
    */
  def invalid(message: String): ValidationResult = new ValidationResult.Builder().input(message).build()

  private def getFirst(context: ProcessContext, props: Seq[PropertyDescriptor]): String = {
    props.toStream.flatMap(p => Option(context.getProperty(p).getValue)).headOption.getOrElse {
      throw new IllegalArgumentException(
        s"Must provide one of the following properties: ${props.map(_.getName).mkString(", ")}")
    }
  }

  /**
    * Full name of a flow file
    *
    * @param f flow file
    * @return
    */
  private def fullName(f: FlowFile): String = f.getAttribute("path") + f.getAttribute("filename")

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
  private def checkCompatibleSchema(existing: SimpleFeatureType, input: SimpleFeatureType): Unit = {
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

  object IngestMode {
    val Converter    = "Converter"
    val AvroDataFile = "AvroDataFile"
  }

  /**
    * Abstraction over ingest methods
    *
    * @param logger component logger
    */
  sealed abstract class Ingest(logger: ComponentLog) extends Closeable {

    /**
      * Ingest a flow file
      *
      * @param fw feature writer
      * @param session session
      * @param flowFile flow file
      */
    def ingest(fw: FeatureWriterSimple, session: ProcessSession, flowFile: FlowFile): Unit = {
      val fullFlowFileName = fullName(flowFile)
      logger.debug(s"Running ${getClass.getName} on file $fullFlowFileName")
      var result = (0L, 0L)
      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          result = ingest(fullFlowFileName, in, fw)
        }
      })
      val (success, failure) = result
      session.putAttribute(flowFile, "geomesa.ingest.successes", success.toString)
      session.putAttribute(flowFile, "geomesa.ingest.failures", failure.toString)
      logger.debug(s"Ingested file $fullFlowFileName with $success successes and $failure failures")
    }

    /**
      * Ingest a flow file
      *
      * @param name file name
      * @param in input stream
      * @param fw feature writer
      * @return (success count, failure count)
      */
    protected def ingest(name: String, in: InputStream, fw: FeatureWriterSimple): (Long, Long)

    /**
      * Log an error from writing a given feature
      *
      * @param sf feature
      * @param e error
      */
    protected def logError(sf: SimpleFeature, e: Throwable): Unit =
      logger.error(s"Error writing feature to store: '${DataUtilities.encodeFeature(sf)}'", e)
  }

  /**
    * GeoAvro ingest
    *
    * @param schema existing simple feature type
    * @param logger component logger
    */
  class AvroIngest(schema: SimpleFeatureType, logger: ComponentLog) extends Ingest(logger) {

    override protected def ingest(name: String, in: InputStream, fw: FeatureWriterSimple): (Long, Long) = {
      var success = 0L
      var failure = 0L
      WithClose(new AvroDataFileReader(in)) { reader =>
        checkCompatibleSchema(schema, reader.getSft)
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
      }
      (success, failure)
    }

    override def close(): Unit = {}
  }

  /**
    * Converter ingest
    *
    * @param sft simple feature type
    * @param config converter config
    * @param logger component logger
    */
  class ConverterIngest(sft: SimpleFeatureType, config: Config, logger: ComponentLog) extends Ingest(logger) {

    private val converters = {
      val factory = new BasePooledObjectFactory[SimpleFeatureConverter] {
        override def create(): SimpleFeatureConverter = SimpleFeatureConverter(sft, config)
        override def wrap(obj: SimpleFeatureConverter): PooledObject[SimpleFeatureConverter] =
          new DefaultPooledObject(obj)
        override def destroyObject(p: PooledObject[SimpleFeatureConverter]): Unit = p.getObject.close()
      }

      val poolConfig = new GenericObjectPoolConfig[SimpleFeatureConverter]()
      poolConfig.setMaxTotal(-1)

      new GenericObjectPool(factory, poolConfig)
    }

    override protected def ingest(name: String, in: InputStream, fw: FeatureWriterSimple): (Long, Long) = {
      val converter = converters.borrowObject()
      try {
        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(name))
        converter.process(in, ec).foreach { sf =>
          try { FeatureUtils.write(fw, sf) } catch {
            case NonFatal(e) =>
              ec.success.inc(-1)
              ec.failure.inc(1)
              logError(sf, e)
          }
        }
        (ec.success.getCount, ec.failure.getCount)
      } finally {
        converters.returnObject(converter)
      }
    }

    override def close(): Unit = converters.close()
  }

  /**
    * Abstraction over feature writers
    */
  sealed trait Writers extends Closeable {

    /**
      * Execute a function against a feature writer
      *
      * @param fn function to execute
      * @tparam T result
      * @return
      */
    def apply[T](fn: FeatureWriterSimple => T): T
  }

  /**
    * Pooled feature writers, re-used between flow files
    *
    * @param ds datastore
    * @param typeName feature type name being written to
    * @param timeout how long to wait between flushes of cached feature writers, in millis
    */
  class PooledWriters(ds: DataStore, typeName: String, timeout: Long) extends Writers {

    private val pool = {
      val factory = new BasePooledObjectFactory[FeatureWriterSimple] {
        override def create(): FeatureWriterSimple = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
        override def wrap(obj: FeatureWriterSimple): PooledObject[FeatureWriterSimple] = new DefaultPooledObject(obj)
        override def destroyObject(p: PooledObject[FeatureWriterSimple]): Unit = CloseWithLogging(p.getObject)
      }

      val config = new GenericObjectPoolConfig[FeatureWriterSimple]()
      config.setMaxTotal(-1)
      config.setMaxIdle(-1)
      config.setMinIdle(0)
      config.setMinEvictableIdleTimeMillis(timeout)
      config.setTimeBetweenEvictionRunsMillis(math.max(1000, timeout / 5))
      config.setNumTestsPerEvictionRun(10)

      new GenericObjectPool(factory, config)
    }

    override def apply[T](fn: FeatureWriterSimple => T): T = {
      val writer = pool.borrowObject()
      try { fn(writer) } finally { pool.returnObject(writer) }
    }

    override def close(): Unit = {
      CloseWithLogging(pool)
      ds.dispose()
    }
  }

  /**
    * Each flow file gets a new feature writer, which is closed after use
    *
    * @param ds datastore
    * @param typeName feature type name being written to
    */
  class SingletonWriters(ds: DataStore, typeName: String) extends Writers {
    override def apply[T](fn: FeatureWriterSimple => T): T = {
      val writer = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
      try { fn(writer) } finally { CloseWithLogging(writer) }
    }
    override def close(): Unit = ds.dispose()
  }

  /**
    * Processor configuration properties
    */
  object Properties {
    val SftName: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("SftName")
          .required(false)
          .description("Choose a simple feature type defined by a GeoMesa SFT Provider (preferred)")
          .allowableValues(SimpleFeatureTypeLoader.listTypeNames.sorted: _*)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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

    val ConverterName: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("ConverterName")
          .required(false)
          .description("Choose an SimpleFeature Converter defined by a GeoMesa SFT Provider (preferred)")
          .allowableValues(ConverterConfigLoader.listConverterNames.sorted: _*)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .build()

    val ConverterSpec: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("ConverterSpec")
          .required(false)
          .description("Manually define a converter using typesafe config")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .addValidator(ConverterValidator)
          .build()

    val ConverterErrorMode: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("ConverterErrorMode")
          .required(false)
          .description("Override the converter error mode behavior")
          .allowableValues(ErrorMode.SkipBadRecords.toString, ErrorMode.RaiseErrors.toString)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .build()

    val IngestModeProp: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("Mode")
          .required(true)
          .description("Ingest mode")
          .allowableValues(IngestMode.Converter, IngestMode.AvroDataFile)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .defaultValue(IngestMode.Converter)
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
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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

  object Relationships {
    final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
    final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
  }
}
