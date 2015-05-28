package org.locationtech.geomesa.nifi

import java.io.InputStream

import com.typesafe.config.ConfigFactory
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.{DataStore, DataStoreFinder, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.nifi.GeoMesaIngestProcessor._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.io.Source

@Tags(Array("geomesa", "geo", "ingest"))
@CapabilityDescription("Ingest Data into GeoMesa")
class GeoMesaIngestProcessor extends AbstractProcessor {

  private var descriptors: java.util.List[PropertyDescriptor] = null
  private var relationships: java.util.Set[Relationship] = null

  private var sft: SimpleFeatureType = null

  protected override def init(context: ProcessorInitializationContext): Unit = {
    descriptors = List(
      Zookeepers,
      InstanceName,
      User,
      Password,
      Catalog,
      FeatureName,
      SftSpec,
      TextSpec).asJava

    relationships = Set(SuccessRelationship, FailureRelationship).asJava
  }

  override def getRelationships = relationships
  override def getSupportedPropertyDescriptors = descriptors

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
    Option(session.get()).map(doWork(context, session, _))

  private def doWork(context: ProcessContext, session: ProcessSession, flowFile: FlowFile): Unit = {
    val name = context.getProperty(FeatureName).getValue
    val spec = context.getProperty(SftSpec).getValue
    val sft = SimpleFeatureTypes.createType(name, spec)

    val ds = getDataStore(context)
    ds.createSchema(sft)

    val converter = getConverter(sft, context)
    val fw = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)

    try {
      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          converter.processInput(Source.fromInputStream(in).getLines()).foreach { sf =>
            val toWrite = fw.next()
            toWrite.setAttributes(sf.getAttributes)
            toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
            toWrite.getUserData.putAll(sf.getUserData)
            fw.write()
          }
        }
      })
      fw.close()
      session.transfer(flowFile, SuccessRelationship)
    } catch {
      case e: Exception =>
        session.transfer(flowFile, FailureRelationship)
    }
  }

  private def getDataStore(context: ProcessContext): DataStore = DataStoreFinder.getDataStore(Map(
    "zookeepers" -> context.getProperty(Zookeepers).getValue,
    "instanceId" -> context.getProperty(InstanceName).getValue,
    "tableName"  -> context.getProperty(Catalog).getValue,
    "user"       -> context.getProperty(User).getValue,
    "password"   -> context.getProperty(Password).getValue
  ))

  private def getConverter(sft: SimpleFeatureType, context: ProcessContext) = {
    val conf = ConfigFactory.parseString(context.getProperty(TextSpec).getValue)
    SimpleFeatureConverters.build[String](sft, conf)
  }

}

object GeoMesaIngestProcessor {
  val Zookeepers = new PropertyDescriptor.Builder()
    .name("Zookeepers")
    .description("Zookeepers host(:port) pairs, comma separated")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val InstanceName = new PropertyDescriptor.Builder()
    .name("Instance")
    .description("Accumulo instance name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val User = new PropertyDescriptor.Builder()
    .name("User")
    .description("Accumulo user name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val Password = new PropertyDescriptor.Builder()
    .name("Password")
    .description("Accumulo password")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .sensitive(true)
    .build

  val Catalog = new PropertyDescriptor.Builder()
    .name("Catalog")
    .description("GeoMesa catalog table name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val FeatureName = new PropertyDescriptor.Builder()
    .name("FeatureName")
    .description("GeoMesa Feature Name")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val SftSpec = new PropertyDescriptor.Builder()
    .name("SftSpec")
    .description("SimpleFeatureType (SFT) spec string")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val TextSpec = new PropertyDescriptor.Builder()
    .name("TextSpec")
    .description("Converter Spec")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
}