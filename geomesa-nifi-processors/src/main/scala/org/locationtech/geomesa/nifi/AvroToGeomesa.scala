package org.locationtech.geomesa.nifi

import java.io.InputStream
import java.util

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.lifecycle.{OnStopped, OnScheduled}
import org.apache.nifi.components.{ValidationResult, ValidationContext, PropertyDescriptor}
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.{Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.nifi.AvroToGeomesa._
import org.locationtech.geomesa.nifi.AbstractGeoMesa._

import scala.collection.JavaConverters._

@Tags(Array("avro", "geomesa", "geo", "ingest", "accumulo"))
@CapabilityDescription("store avro files into geomesa")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
class AvroToGeomesa extends AbstractGeoMesa {

  protected override def init(context: ProcessorInitializationContext): Unit = {
    descriptors = List(
      GeoMesaConfigController,
      Zookeepers,
      InstanceName,
      User,
      Password,
      Catalog,
      FeatureNameOverride).asJava

    relationships = Set(SuccessRelationship, FailureRelationship).asJava
  }


  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    if(useControllerService){
      dataStore = getGeomesaControllerService(context).getDataStore()
    }
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

  @OnStopped
  def cleanup(): Unit = {
    dataStore = null
    getLogger.info("Shut down AvroToGeomesa processor " + getIdentifier)
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {

    val validationFailures: java.util.Collection[ValidationResult] = new util.ArrayList[ValidationResult]()

    val controllerSet:Boolean =  validationContext.getProperty(GeoMesaConfigController).isSet
    useControllerService = controllerSet

    val zooSet:Boolean =         validationContext.getProperty(Zookeepers).isSet
    val instanceSet: Boolean =   validationContext.getProperty(InstanceName).isSet
    val userSet: Boolean =       validationContext.getProperty(User).isSet
    val passSet: Boolean =       validationContext.getProperty(Password).isSet
    val catalogSet: Boolean =    validationContext.getProperty(Catalog).isSet

    // require either controller-service or all of {zoo,instance,user,pw,catalog}
    if(!controllerSet && !(zooSet && instanceSet && userSet && passSet && catalogSet))
      validationFailures.add(new ValidationResult.Builder()
        .input("Use either GeoMesa Configuration Service, or specify accumulo connection parameters.")
        .build)

    validationFailures
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