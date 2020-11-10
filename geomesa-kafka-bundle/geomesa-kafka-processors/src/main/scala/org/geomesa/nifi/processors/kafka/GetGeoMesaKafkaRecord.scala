/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import java.io.Closeable
import java.util.concurrent.{SynchronousQueue, TimeUnit}
import java.util.{Collections, Properties}

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
import org.geomesa.nifi.datastore.processor.AbstractDataStoreProcessor
import org.geomesa.nifi.datastore.processor.Relationships.SuccessRelationship
import org.geomesa.nifi.datastore.processor.records.Properties.GeometrySerializationDefaultWkt
import org.geomesa.nifi.datastore.processor.records.{GeometryEncoding, SimpleFeatureConverterOptions, SimpleFeatureRecordConverter}
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geotools.data._
import org.locationtech.geomesa.kafka.data.{KafkaDataStore, KafkaDataStoreParams}
import org.locationtech.geomesa.kafka.utils.{GeoMessage, GeoMessageProcessor}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.SimpleFeature

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags(Array("kafka", "geomesa", "ingress", "get", "input", "record"))
@CapabilityDescription("Reads Kafka messages from a GeoMesa data source and writes them out as NiFi records")
@WritesAttributes(
  Array(
    new WritesAttribute(attribute = "record.count", description = "The number of records written to the flow file"),
    new WritesAttribute(attribute = "mime.type", description = "The mime-type of the writer used to write the records to the flow file")
  )
)
class GetGeoMesaKafkaRecord extends AbstractProcessor {

  import GetGeoMesaKafkaRecord._

  import scala.collection.JavaConverters._

  private var descriptors: Seq[PropertyDescriptor] = _
  private var relationships: Set[Relationship] = _

  private var converter: SimpleFeatureRecordConverter = _
  private var schema: RecordSchema = _
  private var maxBatchSize = 1000
  private var minBatchSize = 1
  private var maxLatencyMillis: Option[Long] = None
  private var pollTimeout = 1000L
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
    pollTimeout = {
      val prop = context.getProperty(PollTimeout)
      prop.evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).longValue
    }
    val groupId = context.getProperty(GroupId).evaluateAttributeExpressions().getValue

    val ds = {
      val props = {
        val base = AbstractDataStoreProcessor.getDataStoreParams(context, descriptors)
        val offset = context.getProperty(InitialOffset).getValue
        // override/set auto.offset.reset
        val config = KafkaDataStoreParams.ConsumerConfig.lookupOpt(base.asJava).getOrElse(new Properties())
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset)
        base + (KafkaDataStoreParams.ConsumerConfig.key -> KafkaDataStoreParams.ConsumerConfig.text(config))
      }
      lazy val safeToLog = {
        val sensitive = context.getProperties.keySet().asScala.collect { case p if p.isSensitive => p.getName }
        props.map { case (k, v) => s"$k -> ${if (sensitive.contains(k)) { "***" } else { v }}" }
      }
      logger.trace(s"DataStore properties: ${safeToLog.mkString(", ")}")
      DataStoreFinder.getDataStore(props.asJava).asInstanceOf[KafkaDataStore]
    }
    require(ds != null, "Could not load datastore using provided parameters")

    try {
      val sft = ds.getSchema(typeName)
      require(sft != null,
        s"Feature type '$typeName' does not exist in the store. Available types: ${ds.getTypeNames.mkString(", ")}")
      val opts = SimpleFeatureConverterOptions(encoding = encoding, visField = vis, userDataField = userData)
      converter = SimpleFeatureRecordConverter(sft, opts)
      schema = factory.getSchema(Collections.emptyMap[String, String], converter.schema)
      consumer = ds.createConsumer(typeName, groupId, processor)
    } finally {
      CloseWithLogging(ds)
    }

    logger.info("Initialized datastore for Kafka ingress")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val records = processor.ready.poll(pollTimeout, TimeUnit.MILLISECONDS)
    if (records == null) {
      context.`yield`()
    } else {
      val results = records.grouped(maxBatchSize).toSeq.map { group =>
        var flowFile: FlowFile = null
        try {
          flowFile = session.create()
          val result = WithClose(session.write(flowFile)) { out =>
            WithClose(factory.createWriter(logger, schema, out, flowFile)) { writer =>
              writer.beginRecordSet()
              group.foreach { feature =>
                writer.write(converter.convert(feature))
              }
              val result = writer.finishRecordSet()
              WriteResult(result.getRecordCount, writer.getMimeType, result.getAttributes)
            }
          }
          val attributes = new java.util.HashMap[String, String](result.attributes)
          attributes.put(CoreAttributes.MIME_TYPE.key, result.mimeType)
          attributes.put("record.count", String.valueOf(result.count))
          Success(session.putAllAttributes(flowFile, attributes))
        } catch {
          case NonFatal(e) =>
            if (flowFile != null) {
              Try(session.remove(flowFile)).failed.foreach(e.addSuppressed)
            }
            Failure(e)
        }
      }

      val errors = results.collect { case Failure(e) => e }
      if (errors.nonEmpty) {
        try {
          val e = errors.head
          errors.tail.foreach(e.addSuppressed)
          logger.error("Error processing message batch:", e)
          results.collect { case Success(f) => Try(session.remove(f)) }
        } finally {
          processor.done.put(false)
        }
      } else {
        var ok = true
        results.foreach { case Success(f) =>
          try {
            if (f.getAttribute("record.count").toInt <= 0) {
              logger.debug("Removing flow file, no records were written")
              session.remove(f)
            } else {
              session.transfer(f, SuccessRelationship)
            }
          } catch {
            case NonFatal(e) =>
              ok = false
              logger.error(s"Error transferring flow file $f:", e)
              Try(session.remove(f))
          }
        }
        processor.done.put(ok)
      }
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

    val ready = new SynchronousQueue[Seq[SimpleFeature]]()
    val done = new SynchronousQueue[Boolean]()

    private var lastSuccess = System.currentTimeMillis()

    override def consume(records: Seq[GeoMessage]): Boolean = {
      val features = records.collect { case GeoMessage.Change(f) => f }
      if (features.size < minBatchSize && maxLatencyMillis.forall(_ > System.currentTimeMillis() - lastSuccess)) {
        logger.debug(s"Received ${features.size} records but waiting for larger batch")
        false
      } else if (!ready.offer(features, 10, TimeUnit.SECONDS)) {
        logger.warn(s"Received ${features.size} records but onTrigger was not invoked")
        false
      } else if (!done.take) {
        false
      } else {
        lastSuccess = System.currentTimeMillis()
        true
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

  val IncludeVisibilities: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("include-visibilities")
        .displayName("Include Visibilities")
        .description("Include a column with visibility expressions for each row")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("false", "true")
        .defaultValue("false")
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
      .defaultValue("1000")
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
    createPropertyDescriptor(KafkaDataStoreParams.Zookeepers),
    createPropertyDescriptor(KafkaDataStoreParams.ZkPath),
    TypeName,
    GroupId,
    RecordWriter,
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

  case class WriteResult(count: Int, mimeType: String, attributes: java.util.Map[String, String])
}
