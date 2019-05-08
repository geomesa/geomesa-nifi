package org.geomesa.nifi.metrics

import java.io.{InputStream, OutputStream}
import java.util.concurrent.TimeUnit
import java.net.InetSocketAddress
import java.util
import java.util.Date

import com.codahale.metrics.{CachedGauge, MetricFilter, MetricRegistry}
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import org.geomesa.nifi.metrics.DropwizardLatencyMetrics._
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnScheduled, OnStopped}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.StreamCallback
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties.NifiBatchSize
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Relationships.{FailureRelationship, SuccessRelationship}
import org.geomesa.nifi.geo.ConvertToGeoAvro.{ConverterName, ConverterSpec, FeatureNameOverride, SftName, SftSpec}
import org.geomesa.nifi.geo.IngestMode
import org.geomesa.nifi.geo.validators.{ConverterValidator, SimpleFeatureTypeValidator}
import org.geotools.data.Transaction
import org.locationtech.geomesa.convert.{ConfArgs, ConverterConfigLoader, ConverterConfigResolver, EvaluationContext}
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert2
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs, SimpleFeatureTypeLoader}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.control.NonFatal

@Tags(Array("reporting","graphite","metric"))
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends metrics to graphite")
@SupportsBatching
class DropwizardLatencyMetrics extends AbstractProcessor {


  private var descriptors: Seq[PropertyDescriptor] = _
  private var relationships: Set[Relationship] = _
  private var lastTimeSeen:Long = 0
  private def lastTimeGauge = new CachedGauge[Long](30,TimeUnit.SECONDS) {
    override def loadValue: Long = lastTimeSeen
  }
  protected override def init(context:ProcessorInitializationContext): Unit ={
    descriptors = List(
      SftName,
      SftSpec,
      ConverterName,
      ConverterSpec,
      FeatureNameOverride,
      ConverterErrorMode,
      NifiBatchSize,
      MetricsPrefix,
      FieldToExtract,
      LatencyMetricsName,
      GraphiteHost,
      GraphitePort
    )
    relationships = Set(SuccessRelationship,FailureRelationship)
  }

  @volatile
  private var converter: convert2.SimpleFeatureConverter = _

  @volatile
  private var fieldName: String = _

  @volatile
  private var metricName: String = _

  @volatile
  private var metrics: MetricRegistry = _

  @volatile
  private var reporter: GraphiteReporter = _

  @OnScheduled
  def initilize(context: ProcessContext): Unit ={
    val sft = getSft(context)
    converter = getConverter(sft,context)
    fieldName = context.getProperty(FieldToExtract).getValue
    if(!sft.getAttributeDescriptors.exists(_.getLocalName == fieldName)){
      throw new IllegalArgumentException(s"Unable to find field $fieldName in sft ${sft.getTypeName}")
    }
    metricName = context.getProperty(LatencyMetricsName).getValue
    metrics = getRegistry(context)
    // register the 'last time seen' gauge metric
    val lastTime:String = s"${converter.targetSft.getTypeName}.$metricName.lastTime"
    metrics.register(lastTime, lastTimeGauge)

  }

  protected def fullName(f:FlowFile):String = f.getAttribute("path") +f.getAttribute("filename")

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val flowFiles = session.get(context.getProperty(NifiBatchSize).asInteger())
    getLogger.debug(s"Processing ${flowFiles.size()} files in batch")
    val metric =s"${converter.targetSft.getTypeName}.$metricName.latency"
    val successes = new util.ArrayList[FlowFile]()
    if (flowFiles != null && flowFiles.size > 0) {
        flowFiles.asScala.foreach { f =>
          try {
            getLogger.debug(s"Processing file ${fullName(f)}")
            doWork(context,session,f,metric)
            successes.add(f)
          } catch {
            case NonFatal(e) =>
              getLogger.error(s"Error processing file ${fullName(f)}:", e)
              session.transfer(f, FailureRelationship)
          }
        }
    }
    successes.foreach(session.transfer(_, SuccessRelationship))
  }

  private def doWork(context:ProcessContext, session: ProcessSession, flowFile: FlowFile, metric:String): Unit = {
    val newflowFile = session.write(flowFile, new StreamCallback {
      override def process(in: InputStream, out: OutputStream): Unit = {
        val fullflowfilename = fullName(flowFile)
        val ec = converter.createEvaluationContext(Map(EvaluationContext.InputFilePathKey -> fullflowfilename))
        converter.process(in,ec).foreach {sf =>
          lastTimeSeen = System.currentTimeMillis()
          val latency = lastTimeSeen - sf.getAttribute(fieldName).asInstanceOf[Date].getTime
          if (latency>0){
            if(getLogger.isDebugEnabled) getLogger.debug(s"$metric with value ${latency.toString}")
            metrics.histogram(metric).update(latency)
          }else{
            if(getLogger.isDebugEnabled) getLogger.debug(s"$metric with value ${latency.toString} is negative for observation $sf")
          }
        }
      }
    })
  }

  protected def getSft(context: ProcessContext): SimpleFeatureType = {
    val sftArg = Option(context.getProperty(SftName).getValue)
      .orElse(Option(context.getProperty(SftSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${SftName.getName} or ${SftSpec.getName} property"))
    val typeName = context.getProperty(FeatureNameOverride).getValue
    SftArgResolver.getArg(SftArgs(sftArg, typeName)) match {
      case Left(e) => throw e
      case Right(sftype) => sftype
    }
  }

  protected def getConverter(sft: SimpleFeatureType, context: ProcessContext): convert2.SimpleFeatureConverter = {
    val convertArg = Option(context.getProperty(ConverterName).getValue)
      .orElse(Option(context.getProperty(ConverterSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${ConverterName.getName} or ${ConverterSpec.getName} property"))
    val config = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
      case Left(e) => throw e
      case Right(conf) => conf
    }
    SimpleFeatureConverter(sft, config)
  }

  def getRegistry(context: ProcessContext): MetricRegistry = {
    val grh = context.getProperty(GraphiteHost).getValue
    val grp = context.getProperty(GraphitePort).getValue.toInt
    val graphite = new Graphite(new InetSocketAddress(grh,grp))
    val registry = new MetricRegistry
    reporter = GraphiteReporter.forRegistry(registry)
        .prefixedWith(context.getProperty(MetricsPrefix).getValue)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .filter(MetricFilter.ALL)
        .build(graphite)
    reporter.start(10, TimeUnit.SECONDS)
    registry
  }

  //  todo: persist the lastTimeSeen
  @OnStopped
  def cleanup(): Unit = {
    if (metrics != null) {
      metrics = null
    }
    if (reporter != null){
      reporter.stop()
      reporter = null
    }
  }
}

object DropwizardLatencyMetrics {
  val SftName: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("SftName")
      .required(false)
      .description("Choose a simple feature type defined by a GeoMesa SFT Provider (preferred)")
      .allowableValues(SimpleFeatureTypeLoader.listTypeNames.toArray: _*)
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

  val NifiBatchSize: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("BatchSize")
      .required(false)
      .description("Number of FlowFiles to execute in a single batch")
      .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .defaultValue("5")
      .build()
  //      MetricsPrefix,

  val MetricsPrefix: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("MetricsPrefix")
    .required(true)
    .description("graphite metric prefix, (eg path.to.some.)")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build()

  val LatencyMetricsName: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("LatencyMetricsName")
    .required(true)
    .description("LatencyMetricsName (e.g. receiptTime), defaults")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build()

  val FieldToExtract: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("FieldToExtract")
    .required(false)
    .description("Field name to extract")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build()

  val GraphiteHost: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("GraphiteHost")
    .required(true)
    .description("Graphite host name")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build()

  val GraphitePort: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("GraphitePort")
      .required(true)
      .description("Graphite Port ")
      .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
      .defaultValue("2003")
      .build()

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build()
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build()
}