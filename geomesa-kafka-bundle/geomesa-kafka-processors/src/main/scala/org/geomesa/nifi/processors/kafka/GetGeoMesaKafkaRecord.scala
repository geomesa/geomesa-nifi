/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import org.apache.commons.codec.digest.MurmurHash3
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, TriggerWhenEmpty, WritesAttribute, WritesAttributes}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled, OnShutdown, OnStopped}
import org.apache.nifi.components.{PropertyDescriptor, Validator}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.serialization.RecordSetWriterFactory
import org.apache.nifi.serialization.record.RecordSchema
import org.geomesa.nifi.datastore.processor.records.Properties.GeometrySerializationDefaultWkt
import org.geomesa.nifi.datastore.processor.records.{Expressions, GeometryEncoding, SimpleFeatureConverterOptions, SimpleFeatureRecordConverter}
import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geotools.api.feature.`type`.GeometryDescriptor
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.exporters.DelimitedExporter
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult.BatchResult
import org.locationtech.geomesa.kafka.data.{KafkaDataStore, KafkaDataStoreFactory, KafkaDataStoreParams}
import org.locationtech.geomesa.kafka.utils.{GeoMessage, GeoMessageProcessor}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.encodeDescriptor
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.{ByteArrayOutputStream, Closeable}
import java.util.concurrent.{SynchronousQueue, TimeUnit}
import java.util.{Collections, Properties}
import scala.util.Try
import scala.util.control.NonFatal

@TriggerWhenEmpty
@Tags(Array("kafka", "geomesa", "ingress", "get", "input", "record"))
@CapabilityDescription("Reads Kafka messages from a GeoMesa data source and writes them out as NiFi records")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes(
  Array(
    new WritesAttribute(attribute = "record.count", description = "The number of records written to the flow file"),
    new WritesAttribute(attribute = "mime.type", description = "The mime-type of the writer used to write the records to the flow file")
  )
)
class GetGeoMesaKafkaRecord extends AbstractProcessor {

  import GetGeoMesaKafkaRecord._
  import org.geomesa.nifi.datastore.processor.Relationships.SuccessRelationship
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  private var descriptors: Seq[PropertyDescriptor] = _
  private var relationships: Set[Relationship] = _

  private var converter: SimpleFeatureRecordConverter = _
  private var schema: RecordSchema = _
  private var defaultAttributes: java.util.Map[String, String] = _
  private var idGenerator: Option[IdGenerator] = None
  private var maxBatchSize = 10000
  private var minBatchSize = 1
  private var maxLatencyMillis: Option[Long] = None
  private var factory: RecordSetWriterFactory = _
  private var consumer: Closeable = _

  private val processor = new RecordProcessor()

  private def logger: ComponentLog = getLogger

  override protected def init(context: ProcessorInitializationContext): Unit = {
    relationships = Set(SuccessRelationship)
    descriptors = ProcessorDescriptors

    logger.info(s"Props are ${descriptors.mkString(", ")}")
    logger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override def getRelationships: java.util.Set[Relationship] = relationships.asJava
  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = descriptors.asJava

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    logger.info("Initializing")

    def boolean(p: PropertyDescriptor, name: String): Option[String] =
      if (java.lang.Boolean.parseBoolean(context.getProperty(p).getValue)) { Some(name) } else { None }

    val typeName = context.getProperty(TypeName).evaluateAttributeExpressions().getValue
    val encoding =
      GeometryEncoding(context.getProperty(GeometrySerializationDefaultWkt).evaluateAttributeExpressions().getValue)
    val vis = boolean(IncludeVisibilities, "visibilities")
    val userData = boolean(IncludeUserData, "user-data")

    factory = context.getProperty(RecordWriter).asControllerService(classOf[RecordSetWriterFactory])
    maxBatchSize = context.getProperty(RecordMaxBatchSize).evaluateAttributeExpressions().asInteger
    minBatchSize = context.getProperty(RecordMinBatchSize).evaluateAttributeExpressions().asInteger
    maxLatencyMillis = {
      val prop = context.getProperty(RecordMaxLatency)
      Option(prop.evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS)).map(_.longValue)
    }
    val pollTimeout = {
      val prop = context.getProperty(PollTimeout)
      prop.evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).longValue
    }
    val groupId = context.getProperty(GroupId).evaluateAttributeExpressions().getValue

    val ds = {
      val props = {
        val base = GeoMesaDataStoreService.getDataStoreParams(context, descriptors)
        val offset = context.getProperty(InitialOffset).getValue
        // override/set auto.offset.reset
        val config = KafkaDataStoreParams.ConsumerConfig.lookupOpt(base.asJava).getOrElse(new Properties())
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset)
        base ++ Map(KafkaDataStoreParams.ConsumerConfig.key -> KafkaDataStoreParams.ConsumerConfig.text(config))
      }
      lazy val safeToLog = {
        val sensitive = context.getProperties.keySet().asScala.collect { case p if p.isSensitive => p.getName }
        props.map { case (k, v) => s"$k -> ${if (sensitive.contains(k)) { "***" } else { v }}" }
      }
      logger.trace(s"DataStore properties: ${safeToLog.mkString(", ")}")
      KafkaDataStore.LoadIntervalProperty.threadLocalValue.set(s"$pollTimeout ms")
      try {
        GeoMesaDataStoreService.tryGetDataStore[KafkaDataStoreFactory](props.asJava).get.asInstanceOf[KafkaDataStore]
      } finally {
        KafkaDataStore.LoadIntervalProperty.threadLocalValue.remove()
      }
    }

    try {
      val sft = ds.getSchema(typeName)
      require(sft != null,
        s"Feature type '$typeName' does not exist in the store. Available types: ${ds.getTypeNames.mkString(", ")}")
      val opts = SimpleFeatureConverterOptions(encoding = encoding, visField = vis, userDataField = userData)

      // gather default variables to reduce configuration on record set writer
      defaultAttributes = {
        val attributes = new java.util.HashMap[String, String]()
        attributes.put(Expressions.IdCol.attribute, SimpleFeatureRecordConverter.DefaultIdCol)
        val geoms = sft.getAttributeDescriptors.asScala.collect { case d: GeometryDescriptor => encodeDescriptor(sft, d) }
        if (geoms.nonEmpty) {
          attributes.put(Expressions.GeomCols.attribute, geoms.mkString(","))
        }
        val json = sft.getAttributeDescriptors.asScala.collect { case d if d.isJson => d.getLocalName }
        if (json.nonEmpty) {
          attributes.put(Expressions.JsonCols.attribute, json.mkString(","))
        }
        sft.getDtgField.foreach(attributes.put(Expressions.DtgCol.attribute, _))
        vis.foreach(attributes.put(Expressions.VisCol.attribute, _))
        Collections.unmodifiableMap(attributes)
      }
      if (java.lang.Boolean.parseBoolean(context.getProperty(ReplaceFeatureIds).getValue)) {
        idGenerator = Some(new IdGenerator(sft))
      } else {
        idGenerator = None
      }
      converter = SimpleFeatureRecordConverter(sft, opts)
      schema = factory.getSchema(Collections.emptyMap(), converter.schema)
      consumer = ds.createConsumer(typeName, groupId, processor)
    } finally {
      CloseWithLogging(ds)
    }

    logger.info("Initialized datastore for Kafka ingress")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val records = processor.ready.poll(1, TimeUnit.SECONDS)
    if (records == null) {
      context.`yield`()
    } else {
      records.grouped(maxBatchSize).toSeq.foreach { group =>
        var flowFile: FlowFile = null
        try {
          flowFile = session.create()
          val errors = new ErrorHolder()
          val (result, mimeType) = WithClose(session.write(flowFile)) { out =>
            WithClose(factory.createWriter(logger, schema, out, defaultAttributes)) { writer =>
              writer.beginRecordSet()
              group.foreach { feature =>
                try {
                  val record = converter.convert(feature)
                  idGenerator.foreach { g =>
                    record.setValue(SimpleFeatureRecordConverter.DefaultIdCol, g.id(feature))
                  }
                  writer.write(record)
                } catch {
                  case NonFatal(e) =>
                    logger.warn(s"Error writing record for feature ${feature.getID}: ", e)
                    errors.error(e)
                }
              }
              (writer.finishRecordSet(), writer.getMimeType)
            }
          }

          if (result.getRecordCount > 0) {
            val attributes = new java.util.HashMap[String, String](result.getAttributes)
            attributes.put(CoreAttributes.MIME_TYPE.key, mimeType)
            attributes.put("record.count", String.valueOf(result.getRecordCount))
            attributes.put("record.errors", String.valueOf(errors.count))
            flowFile = session.putAllAttributes(flowFile, attributes)
            session.transfer(flowFile, SuccessRelationship)
          } else {
            session.remove(flowFile)
            logger.error(s"File produced 0 valid and ${errors.count} invalid records")
            errors.first.foreach { e =>
              logger.error(s"First record exception:", e)
            }
          }
        } catch {
          case NonFatal(e) =>
            Option(flowFile).flatMap(f => Try(session.remove(f)).failed.filter(_ != e).toOption).foreach(e.addSuppressed)
            logger.error("Error processing message batch:", e)
        }
      }

      processor.done.put(true)
    }
  }

  @OnRemoved
  @OnStopped
  @OnShutdown
  def cleanup(): Unit = {
    logger.info("Processor shutting down")
    val start = System.currentTimeMillis()
    if (consumer != null) {
      CloseWithLogging(consumer)
      consumer = null
    }
    logger.info(s"Shut down in ${System.currentTimeMillis() - start}ms")
  }

  class RecordProcessor extends GeoMessageProcessor {

    // synchronous queues for passing data to onTrigger and back
    val ready = new SynchronousQueue[Seq[SimpleFeature]]()
    val done = new SynchronousQueue[Boolean]()

    private var lastSuccess = System.currentTimeMillis()

    // note: this needs to complete before max.poll.interval.ms, which defaults to 5 minutes
    override def consume(records: Seq[GeoMessage]): BatchResult = {
      val features = records.collect { case GeoMessage.Change(f) => f }

      // if we've read too many messages but haven't processed them (e.g. due to back-pressure),
      // pause the kafka consumers so that we don't pile up results in memory
      def continueOrPause: BatchResult = {
        if (features.size < maxBatchSize * 10) { BatchResult.Continue } else {
          logger.warn(s"Received ${features.size} records - pausing consumers while waiting for onTrigger")
          BatchResult.Pause
        }
      }

      // 1. offer features with `ready.offer` and wait for onTrigger to read them
      // 2. wait for onTrigger to finish with `done.take`

      if (features.size < minBatchSize && maxLatencyMillis.forall(_ > System.currentTimeMillis() - lastSuccess)) {
        logger.debug(s"Received ${features.size} records but waiting for larger batch")
        BatchResult.Continue // retry after checking for more messages
      } else if (!ready.offer(features, 10, TimeUnit.SECONDS)) {
        // onTrigger was not invoked, retry the batch later
        logger.warn(s"Received ${features.size} records but onTrigger was not invoked")
        continueOrPause
      } else if (!done.take) {
        // error in onTrigger, retry the batch later
        continueOrPause
      } else {
        // commit offsets since we've successfully processed the messages
        lastSuccess = System.currentTimeMillis()
        BatchResult.Commit
      }
    }
  }
}

object GetGeoMesaKafkaRecord extends PropertyDescriptorUtils {

  import org.apache.nifi.components.PropertyDescriptor
  import org.apache.nifi.serialization.RecordSetWriterFactory

  val TypeName: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("type-name")
        .displayName("Type Name")
        .description("The schema type name to read")
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(true)
        .build

  val GroupId: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("kafka-group-id")
        .displayName("Kafka Group ID")
        .description("The unique group ID used to track position in the Kafka data stream")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(true)
        .build

  val InitialOffset: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("kafka-initial-offset")
        .displayName("Kafka Initial Offset")
        .description("The initial position to start reading from the Kafka data stream")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .allowableValues("latest", "earliest")
        .defaultValue("latest")
        .build

  val RecordWriter: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("record-writer")
      .displayName("Record Writer")
      .description("The Record Writer to use in order to serialize the data before writing to a FlowFile")
      .identifiesControllerService(classOf[RecordSetWriterFactory])
      .required(true)
      .build

  val ReplaceFeatureIds: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("replace-fids")
        .displayName("Replace Feature IDs")
        .description("Replace the Kafka feature ID with a new ID based on a hash of the record fields")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("false", "true")
        .defaultValue("true")
        .build

  val IncludeVisibilities: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("include-visibilities")
        .displayName("Include Visibilities")
        .description("Include a column with visibility expressions for each row")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("false", "true")
        .defaultValue("true")
        .build

  val IncludeUserData: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("include-user-data")
        .displayName("Include User Data")
        .description("Include a column with user data from the SimpleFeature, serialized as JSON")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("false", "true")
        .defaultValue("false")
        .build

  val RecordMaxBatchSize: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("record-max-batch-size")
      .displayName("Record Maximum Batch Size")
      .description("The maximum number of records to write to a single FlowFile")
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
      .defaultValue("10000")
      .build()

  val RecordMinBatchSize: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("record-min-batch-size")
        .displayName("Record Minimum Batch Size")
        .description("The minimum number of records to write to a single FlowFile")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("1")
        .build()

  val RecordMaxLatency: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("record-max-latency")
        .displayName("Record Max Latency")
        .description("The maximum latency before outputting a record to a FlowFile. This may override the minimum batch size, if both are set")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .required(false)
        .build()

  val PollTimeout: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("consumer-poll-timeout")
        .displayName("Consumer Poll Timeout")
        .description("How long to wait for the next Kafka record before closing a FlowFile")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("1 second")
        .build()

  val ProcessorDescriptors: Seq[PropertyDescriptor] = Seq(
    createPropertyDescriptor(KafkaDataStoreParams.Brokers),
    createPropertyDescriptor(KafkaDataStoreParams.Catalog),
    createPropertyDescriptor(KafkaDataStoreParams.Zookeepers),
    createPropertyDescriptor(KafkaDataStoreParams.ZkPath),
    TypeName,
    GroupId,
    RecordWriter,
    ReplaceFeatureIds,
    GeometrySerializationDefaultWkt,
    IncludeVisibilities,
    IncludeUserData,
    RecordMaxBatchSize,
    RecordMinBatchSize,
    RecordMaxLatency,
    PollTimeout,
    InitialOffset,
    createPropertyDescriptor(KafkaDataStoreParams.ConsumerCount),
    createPropertyDescriptor(KafkaDataStoreParams.ConsumerConfig)
  )

  /**
   * Id generator - not thread safe
   *
   * @param sft simple feature type
   */
  class IdGenerator(sft: SimpleFeatureType) {

    private val stream = new ByteArrayOutputStream()

    // assertion - we don't need to close this as it just closes the underlying stream, which is a byte array
    private val exporter = DelimitedExporter.csv(stream, withHeader = false, includeIds = true)
    exporter.start(sft)

    private val buf = Array.ofDim[Byte](16)

    def id(feature: SimpleFeature): String = {
      stream.reset()
      exporter.export(Iterator.single(feature))
      val Array(lo, hi) = MurmurHash3.hash128(stream.toByteArray)
      ByteArrays.writeLong(lo, buf)
      ByteArrays.writeLong(hi, buf, 8)
      ByteArrays.toHex(buf, 0, 16)
    }
  }

  class ErrorHolder {
    private var errors = 0
    private var err: Throwable = _

    def count: Int = errors
    def first: Option[Throwable] = Option(err)

    def error(e: Throwable): Unit = {
      errors += 1
      if (err == null) {
        err = e
      }
    }
  }
}
