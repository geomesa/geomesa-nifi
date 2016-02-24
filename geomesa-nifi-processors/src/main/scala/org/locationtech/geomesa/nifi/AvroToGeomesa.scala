package org.locationtech.geomesa.nifi

import java.io.InputStream
import javafx.scene.input.ZoomEvent

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
import org.locationtech.geomesa.nifi.AbstractGeoMesa._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest"))
@CapabilityDescription("store avro files into geomesa")
class AvroToGeomesa extends AbstractGeoMesa {

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

  protected def getDataStore(context: ProcessContext): DataStore = DataStoreFinder.getDataStore(Map(
    "zookeepers" -> context.getProperty(Zookeepers).getValue,
    "instanceId" -> context.getProperty(InstanceName).getValue,
    "tableName"  -> context.getProperty(Catalog).getValue,
    "user"       -> context.getProperty(User).getValue,
    "password"   -> context.getProperty(Password).getValue
  ))

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    dataStore = getDataStore(context)
    getLogger.info(s"Initialized Avro GeoMesa datastore")
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
            val featureWriter = dataStore.getFeatureWriterAppend(reader.getSft.getTypeName, Transaction.AUTO_COMMIT)
            try {
              reader.foreach { sf =>
                val toWrite = featureWriter.next()
                toWrite.setAttributes(sf.getAttributes)
                toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
                toWrite.getUserData.putAll(sf.getUserData)
                featureWriter.write()
              }
            } finally {
              featureWriter.close()
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

}

object AvroToGeomesa {

  val FeatureNameOverride = new PropertyDescriptor.Builder()
    .name("FeatureNameOverride")
    .description("Override the Simple Feature Type name from the SFT Spec")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)  // TODO validate
    .build

}