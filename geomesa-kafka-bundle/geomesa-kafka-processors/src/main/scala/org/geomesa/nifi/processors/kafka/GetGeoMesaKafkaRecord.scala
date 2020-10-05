/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import java.util.concurrent.{SynchronousQueue, TimeUnit}
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, TriggerWhenEmpty, WritesAttribute, WritesAttributes}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled, OnShutdown, OnStopped}
import org.apache.nifi.components.{PropertyDescriptor, Validator}
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.serialization.record.{Record, RecordSchema}
import org.apache.nifi.serialization.{RecordSetWriter, RecordSetWriterFactory}
import org.geomesa.nifi.datastore.processor.AbstractGeoIngestProcessor
import org.geomesa.nifi.datastore.processor.AbstractGeoIngestProcessor.Relationships.SuccessRelationship
import org.geomesa.nifi.datastore.processor.records.{GeometryEncoding, SimpleFeatureRecordConverter}
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geotools.data._
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.{KafkaFeatureChanged, KafkaFeatureCleared, KafkaFeatureRemoved}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.annotation.tailrec
import scala.util.control.NonFatal

@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags(Array("kafka", "geomesa", "ingress", "get", "input"))
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

  private var ds: DataStore = _
  private var fs: FeatureSource[SimpleFeatureType, SimpleFeature] = _
  private var converter: SimpleFeatureRecordConverter = _
  private var schema: RecordSchema = _
  private var maxBatchSize = 1000
  private var minBatchSize = 1
  private var maxLatencyMillis: Option[Long] = None
  private var pollTimeout = 1000L
  private var factory: RecordSetWriterFactory = _

  // we use a synchronous queue to ensure the consumer tracks offsets correctly
  private val queue = new SynchronousQueue[SimpleFeature]()
  private val listener = new RecordFeatureListener()

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

    val typeName = context.getProperty(TypeName).evaluateAttributeExpressions().getValue
    val encoding = context.getProperty(GeometrySerialization).getValue match {
      case GetGeoMesaKafkaRecord.Wkt => GeometryEncoding.Wkt
      case GetGeoMesaKafkaRecord.Wkb => GeometryEncoding.Wkb
      case s => throw new IllegalArgumentException(s"Unexpected value for '${GeometrySerialization.getName}': $s")
    }
    val vis = java.lang.Boolean.parseBoolean(context.getProperty(IncludeVisibilities).getValue)

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

    ds = {
      val props = {
        val base = AbstractGeoIngestProcessor.getDataStoreParams(context, descriptors) ++ Map(
          // disable feature caching since we are just using the listeners
          KafkaDataStoreParams.CacheExpiry.key -> "0s"
        )
        val groupId = context.getProperty(GroupId).evaluateAttributeExpressions().getValue
        val offset = context.getProperty(InitialOffset).getValue
        // override/set group id and auto.offset.reset
        val config = KafkaDataStoreParams.ConsumerConfig.lookupOpt(base.asJava).getOrElse(new Properties())
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset)
        base + (KafkaDataStoreParams.ConsumerConfig.key -> KafkaDataStoreParams.ConsumerConfig.text(config))
      }
      lazy val safeToLog = {
        val sensitive = context.getProperties.keySet().asScala.collect { case p if p.isSensitive => p.getName }
        props.map { case (k, v) => s"$k -> ${if (sensitive.contains(k)) { "***" } else { v }}" }
      }
      logger.trace(s"DataStore properties: ${safeToLog.mkString(", ")}")
      DataStoreFinder.getDataStore(props.asJava)
    }
    require(ds != null, "Could not load datastore using provided parameters")

    try {
      val sft = ds.getSchema(typeName)
      require(sft != null,
        s"Feature type '$typeName' does not exist in the store. Available types: ${ds.getTypeNames.mkString(", ")}")
      converter = SimpleFeatureRecordConverter(sft, encoding, vis)
      schema = factory.getSchema(Collections.emptyMap[String, String], converter.schema)
      fs = ds.getFeatureSource(typeName)
      fs.addFeatureListener(listener)
    } catch {
      case NonFatal(e) => CloseWithLogging(ds); ds = null; throw e
    }

    logger.info("Initialized datastore for Kafka ingress")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val record = readNextRecord()
    if (record != null) {
      val flowFile = session.create()
      try {
        val result = WithClose(session.write(flowFile)) { out =>
          WithClose(factory.createWriter(logger, schema, out, flowFile)) { writer =>
            writer.beginRecordSet()
            val count = writer.write(record).getRecordCount
            if (count < maxBatchSize) {
              val timeout = maxLatencyMillis.map(_ + System.currentTimeMillis()).getOrElse(Long.MaxValue)
              writeRemaining(writer, timeout, count)
            }
            val result = writer.finishRecordSet()
            writer.flush()
            WriteResult(result.getRecordCount, writer.getMimeType, result.getAttributes)
          }
        }

        if (result.count <= 0) {
          logger.debug("Removing flow file, no records were written")
          session.remove(flowFile)
        } else {
          val attributes = new java.util.HashMap[String, String](result.attributes)
          attributes.put(CoreAttributes.MIME_TYPE.key, result.mimeType)
          attributes.put("record.count", String.valueOf(result.count))
          session.transfer(session.putAllAttributes(flowFile, attributes), SuccessRelationship)
        }
      } catch {
        case NonFatal(e) =>
          logger.error("Error onTrigger:", e)
          session.remove(flowFile)
      }
    }
  }

  @tailrec
  private def writeRemaining(writer: RecordSetWriter, timeout: Long, count: Int): Unit = {
    if (timeout > System.currentTimeMillis()) {
      val record = readNextRecord()
      if (record != null) {
        val count = writer.write(record).getRecordCount
        if (count < maxBatchSize) {
          writeRemaining(writer, timeout, count)
        }
      } else if (count < minBatchSize) {
        writeRemaining(writer, timeout, count)
      }
    }
  }

  private def readNextRecord(): Record = {
    val feature = queue.poll(pollTimeout, TimeUnit.MILLISECONDS)
    if (feature == null) { null } else {
      converter.convert(feature)
    }
  }

  @OnRemoved
  @OnStopped
  @OnShutdown
  def cleanup(): Unit = {
    logger.info("Processor shutting down")
    val start = System.currentTimeMillis()
    if (ds != null) {
      fs.removeFeatureListener(listener)
      CloseWithLogging(ds)
      fs = null
      ds = null
    }
    logger.info(s"Shut down in ${System.currentTimeMillis() - start}ms")
  }

  class RecordFeatureListener extends FeatureListener {
    override def changed(event: FeatureEvent): Unit = {
      event match {
        case e: KafkaFeatureChanged => queue.put(e.feature)
        case _: KafkaFeatureRemoved => // no-op
        case _: KafkaFeatureCleared => // no-op
        case e                      => logger.error(s"Unknown event $e")
      }
    }
  }
}

object GetGeoMesaKafkaRecord extends PropertyDescriptorUtils {

  import org.apache.nifi.components.PropertyDescriptor
  import org.apache.nifi.serialization.RecordSetWriterFactory

  private val Wkt = "WKT (well-known text)"
  private val Wkb = "WKB (well-known binary)"

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

  val GeometrySerialization: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("geometry-serialization")
        .displayName("Geometry Serialization Format")
        .description("The format to use for serializing geometries")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .allowableValues(Wkt, Wkb)
        .defaultValue(Wkt)
        .build

  val IncludeVisibilities: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("include-visibilities")
        .displayName("Include Visibilities")
        .description("Include a column with visibility expressions for each row")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
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
    GeometrySerialization,
    IncludeVisibilities,
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
