package org.locationtech.geomesa.nifi

import java.io.InputStream

import com.typesafe.config.ConfigFactory
import org.apache.avro.file.DataFileStream
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataUtilities, DataStoreFinder, Transaction}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.feature.{AvroSimpleFeature, FeatureSpecificReader}
import org.locationtech.geomesa.nifi.AvroToGeoToolsDatastore._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest"))
@CapabilityDescription("store avro files into geomesa")
class AvroToGeoToolsDatastore extends AbstractProcessor {

  private var descriptors: java.util.List[PropertyDescriptor] = null
  private var relationships: java.util.Set[Relationship] = null

  private var sft: SimpleFeatureType = null

  protected override def init(context: ProcessorInitializationContext): Unit = {
    relationships = Set(SuccessRelationship, FailureRelationship).asJava
    descriptors = List(DataStoreName, SftConfig).asJava
  }

  override def getRelationships = relationships
  override def getSupportedPropertyDescriptors = descriptors

  override def getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String): PropertyDescriptor = {
    return new PropertyDescriptor.Builder()
    .description("Sets the value on the datastore")
    .name(propertyDescriptorName)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .sensitive(sensitiveProps.contains(propertyDescriptorName))
    .dynamic(true)
    .expressionLanguageSupported(false)
    .build()
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
    Option(session.get()).map(doWork(context, session, _))

  private def doWork(context: ProcessContext, session: ProcessSession, flowFile: FlowFile): Unit = {
    val sft = getSft(context)
    val ds = getDataStore(context)
    ds.createSchema(sft)

    try {
      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
//          val fw = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
//          try {
//            val dfs = new DataFileStream[AvroSimpleFeature](in, new FeatureSpecificReader(sft))
//            getLogger.info("0")
//            dfs.iterator().toList.foreach { sf =>
//              getLogger.info("1")
//              fw.hasNext
//              val toWrite = fw.next()
//              getLogger.info("2")
//              toWrite.setAttributes(sf.getAttributes)
//              getLogger.info("3")
//              toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
//              getLogger.info("4")
//              toWrite.getUserData.putAll(sf.getUserData)
//              getLogger.info("5")
//              fw.write()
//              getLogger.info("6")
//              session.adjustCounter("featuresWritten", 1L, false)
//            }
//          } finally {
//            fw.close()
//          }
          val dfs = new DataFileStream[AvroSimpleFeature](in, new FeatureSpecificReader(sft))
          val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          val c = new DefaultFeatureCollection()
          c.addAll(dfs.iterator().toList)
          fs.addFeatures(c)
        }
      })
      session.transfer(flowFile, SuccessRelationship)
    } catch {
      case e: Exception =>
        getLogger.error("error", e)
        session.transfer(flowFile, FailureRelationship)
    }
  }

  val sensitiveProps = listDataStores().map(_.getParametersInfo.filter(_.isPassword).map(_.getName)).flatten

  private def getDataStore(context: ProcessContext) = {
    val dsProps = context.getProperties.filter(_._1.getName != DataStoreName.getName).map { case (a, b) => a.getName -> b }
    getLogger.info(s"Looking for datastore with props $dsProps")
    DataStoreFinder.getDataStore(dsProps)
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {
    val validationResults = scala.collection.mutable.ListBuffer.empty[ValidationResult]
    val dsOpt = Option(validationContext.getProperty(DataStoreName).getValue)

    if (dsOpt.isDefined && dsOpt.get.nonEmpty) {
      val dsName = dsOpt.get
      getLogger.debug(s"Attemping to validate params for datastore $dsName")
      val dsParams = listDataStores().filter(_.getDisplayName == dsName).toSeq.head.getParametersInfo
      val required = dsParams.filter(_.isRequired)

      val props = validationContext.getProperties.filterKeys(_ != DataStoreName.getName).map { case (a, b) => a.getName -> b }
      val propNames = props.keys

      val missing = required.map(_.getName).toList.filterNot(propNames.contains(_))
      missing.foreach { mp =>
        validationResults +=
          new ValidationResult.Builder()
            .input(mp)
            .valid(false)
            .explanation(s"Required property $mp for datastore $dsName is missing")
            .build()
      }
    } else {
      validationResults +=
        new ValidationResult.Builder()
          .input(DataStoreName.getName)
          .valid(false)
          .explanation(s"Must define available data store name first")
          .build()
    }
    validationResults.asJavaCollection
  }

  private def getSft(context: ProcessContext) =
    SimpleFeatureTypes.createType(ConfigFactory.parseString(context.getProperty(SftConfig).getValue))

}

object AvroToGeoToolsDatastore {

  private def listDataStores() =
    DataStoreFinder.getAvailableDataStores

  val DataStoreName = new PropertyDescriptor.Builder()
    .name("DataStoreName")
    .description("DataStoreName")
    .allowableValues(listDataStores().map(_.getDisplayName).toArray: _*)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val SftConfig = new PropertyDescriptor.Builder()
    .name("SftConfig")
    .description("SimpleFeatureType (SFT) config")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
}

