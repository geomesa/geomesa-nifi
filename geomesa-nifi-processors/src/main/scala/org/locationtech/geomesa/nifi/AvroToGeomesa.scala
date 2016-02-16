package org.locationtech.geomesa.nifi

import java.io.InputStream

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.{DataStore, DataStoreFinder, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.nifi.AvroToGeomesa._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest"))
@CapabilityDescription("store avro files into geomesa")
class AvroToGeomesa extends AbstractProcessor {

  private var descriptors: java.util.List[PropertyDescriptor] = null
  private var relationships: java.util.Set[Relationship] = null

  protected override def init(context: ProcessorInitializationContext): Unit = {
    descriptors = List(
      Zookeepers,
      InstanceName,
      User,
      Password,
      Catalog,
      FeatureNameOverride).asJava

    relationships = Set(SuccessRelationship, FailureRelationship).asJava
  }

  override def getRelationships = relationships
  override def getSupportedPropertyDescriptors = descriptors

  @volatile
  private var dataStore: DataStore = null

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    dataStore = getDataStore(context)
    getLogger.info(s"Initialized GeoMesaIngestProcessor datastore")
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
    Option(session.get()).foreach(doWork(context, session, _))

  private def doWork(context: ProcessContext, session: ProcessSession, flowFile: FlowFile): Unit = {
    try {
      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          val reader = new AvroDataFileReader(in)
          try {
            dataStore.createSchema(reader.getSft)
            val fw = dataStore.getFeatureWriterAppend(reader.getSft.getTypeName, Transaction.AUTO_COMMIT)
            try {
              reader.foreach { sf =>
                val toWrite = fw.next()
                toWrite.setAttributes(sf.getAttributes)
                toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
                toWrite.getUserData.putAll(sf.getUserData)
                fw.write()
              }
            } finally {
              fw.close()
            }
          } finally {
            reader.close()
          }
        }
      })
      session.transfer(flowFile, SuccessRelationship)
    } catch {
      case e: Exception =>
        getLogger.error("error", e)
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

}

object AvroToGeomesa {
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

  val FeatureNameOverride = new PropertyDescriptor.Builder()
    .name("FeatureNameOverride")
    .description("Override the Simple Feature Type name from the SFT Spec")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)  // TODO validate
    .build

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
}