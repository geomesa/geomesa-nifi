/***********************************************************************
 * Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.geo

import java.io.InputStream
import java.util
import java.util.Collections

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, ObjectPool, PooledObject}
import org.apache.nifi.annotation.lifecycle.{OnDisabled, OnRemoved, OnScheduled}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.FeatureWriterSimple
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Relationships._
import org.geomesa.nifi.geo.validators.{ConverterValidator, SimpleFeatureTypeValidator}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data._
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigLoader, ConverterConfigResolver}
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

  @volatile
  private var dataStore: DataStore = _
  @volatile
  private var sft: SimpleFeatureType = _
  @volatile
  private var mode: String = _
  @volatile
  private var converterPool: ObjectPool[SimpleFeatureConverter] = _

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
      NifiBatchSize
    ) ++ dataStoreProperties
    logger.info(s"Props are ${descriptors.mkString(", ")}")
    logger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override def getRelationships: java.util.Set[Relationship] = relationships.asJava
  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = descriptors.asJava

  @OnScheduled
  protected def initialize(context: ProcessContext): Unit = {
    // Data store comes first...then getSft because
    // oddly enough sometimes you want to modify the sft
    dataStore = loadDataStore(context, Map.empty)
    require(dataStore != null, "Fatal error: datastore is null")
    sft = loadSft(context)

    if (dataStore.getTypeNames.contains(sft.getTypeName)) {
      checkCompatibleSchema(sft)
    } else {
      logger.info(s"Creating schema '${sft.getTypeName}'... " +
          s"existing types are ${dataStore.getTypeNames.mkString(", ")}")
      dataStore.createSchema(sft)
    }

    mode = context.getProperty(IngestModeProp).getValue
    if (IngestMode.Converter == mode) {
      initializeConverterPool(context)
    }
    logger.info(s"Initialized datastore ${dataStore.getClass.getSimpleName} with SFT ${sft.getTypeName} in mode $mode")
  }

  private def initializeConverterPool(context: ProcessContext): Unit = {
    val convertArg = Option(context.getProperty(ConverterName).getValue)
      .orElse(Option(context.getProperty(ConverterSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${ConverterName.getName} or ${ConverterSpec.getName} property"))
    var config = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
      case Left(e) => throw e
      case Right(conf) => conf
    }
    Option(context.getProperty(ConverterErrorMode).getValue).foreach { mode =>
      val opts = ConfigValueFactory.fromMap(Collections.singletonMap("error-mode", mode))
      config = ConfigFactory.empty().withValue("options", opts).withFallback(config)
    }

    converterPool = new GenericObjectPool[SimpleFeatureConverter](
      new BasePooledObjectFactory[SimpleFeatureConverter] {
        override def create(): SimpleFeatureConverter = SimpleFeatureConverter(sft, config)
        override def wrap(obj: SimpleFeatureConverter): PooledObject[SimpleFeatureConverter] = new DefaultPooledObject[SimpleFeatureConverter](obj)
        override def destroyObject(p: PooledObject[SimpleFeatureConverter]): Unit = p.getObject.close()
      })
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {
    val result = new util.ArrayList[ValidationResult]()

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
      WithClose(dataStore.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { fw =>
        flowFiles.asScala.foreach { f =>
          try {
            logger.debug(s"Processing file ${fullName(f)}")
            mode match {
              case IngestMode.Converter => converterIngest(fw, session, f)
              case IngestMode.AvroDataFile => avroIngest(fw, session, f)
              case _ => throw new IllegalStateException(s"Unknown ingest type: $mode")
            }
            session.transfer(f, SuccessRelationship)
          } catch {
            case NonFatal(e) =>
              logger.error(s"Error processing file ${fullName(f)}:", e)
              session.transfer(f, FailureRelationship)
          }
        }
      }
    }
  }

  @OnRemoved
  @OnDisabled
  def cleanup(): Unit = {
    if (converterPool != null) {
      CloseWithLogging(converterPool)
      converterPool = null
    }
    if (dataStore != null) {
      dataStore.dispose()
      dataStore = null
    }
    logger.info(s"Shut down ${getClass.getName} processor $getIdentifier")
  }

  /**
    * Loads a simple feature type from the environment, based on the configured NiFi properties
    *
    * @param context context
    * @return
    */
  protected def loadSft(context: ProcessContext): SimpleFeatureType = {
    val sftArg = Option(context.getProperty(SftName).getValue)
      .orElse(Option(context.getProperty(SftSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${SftName.getName} or ${SftSpec.getName} property"))
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
    }.toMap
    val props = configured ++ static // note: static props take precedence
    logger.trace(s"DataStore properties: $props")
    DataStoreFinder.getDataStore(props.asJava)
  }

  /**
    * Avro ingest
    *
    * @param fw feature writer
    * @param session nifi session
    * @param flowFile flow file to ingest
    */
  private def avroIngest(fw: FeatureWriterSimple, session: ProcessSession, flowFile: FlowFile): Unit = {
    logger.debug("Running avro-based ingest")
    val fullFlowFileName = fullName(flowFile)
    session.read(flowFile, new InputStreamCallback {
      override def process(in: InputStream): Unit = {
        WithClose(new AvroDataFileReader(in)) { reader =>
          checkCompatibleSchema(reader.getSft)
          reader.foreach { sf =>
            try {
              FeatureUtils.copyToWriter(fw, sf)
              fw.write()
            } catch {
              case NonFatal(e) =>
                logger.warn(s"ERROR writing feature to DataStore '${DataUtilities.encodeFeature(sf)}'", e)
            }
          }
        }
      }
    })
    logger.debug(s"Ingested avro file $fullFlowFileName")
  }

  /**
    * Converter ingest
    *
    * @param fw feature writer
    * @param session nifi session
    * @param flowFile flow file to ingest
    */
  private def converterIngest(fw: FeatureWriterSimple, session: ProcessSession, flowFile: FlowFile): Unit = {
    logger.debug("Running converter-based ingest")
    val converter = converterPool.borrowObject()
    try {
      val fullFlowFileName = fullName(flowFile)
      val ec = converter.createEvaluationContext(Map("inputFilePath" -> fullFlowFileName))
      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          logger.debug(s"Converting path $fullFlowFileName")
          converter.process(in, ec).foreach { sf =>
            try {
              FeatureUtils.copyToWriter(fw, sf)
              fw.write()
            } catch {
              case NonFatal(e) =>
                ec.counter.incSuccess(-1)
                ec.counter.incFailure(1)
                logger.warn(s"ERROR writing feature to DataStore '${DataUtilities.encodeFeature(sf)}'", e)
            }
          }
        }
      })
      logger.debug(s"Converted and ingested file $fullFlowFileName with ${ec.counter.getSuccess} successes and " +
        s"${ec.counter.getFailure} failures")
    } finally {
      converterPool.returnObject(converter)
    }
  }

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
    * @param input input simple feature type
    */
  private def checkCompatibleSchema(input: SimpleFeatureType): Unit = {
    val existing = dataStore.getSchema(input.getTypeName)
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

  /**
    * Full name of a flow file
    *
    * @param f flow file
    * @return
    */
  private def fullName(f: FlowFile): String = f.getAttribute("path") + f.getAttribute("filename")
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
  }

  object Relationships {
    final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
    final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
  }
}
