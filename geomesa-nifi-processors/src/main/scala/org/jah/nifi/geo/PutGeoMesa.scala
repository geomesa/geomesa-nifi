package org.jah.nifi.geo

import java.util

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.{DataStore, DataStoreFinder}
import org.jah.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.jah.nifi.geo.PutGeoMesa._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "convert", "accumulo", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
class PutGeoMesa extends AbstractGeoIngestProcessor {

  protected override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)
    descriptors = (getPropertyDescriptors ++ List(
      GeoMesaConfigController,
      Zookeepers,
      InstanceName,
      User,
      Password,
      Catalog
     )).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  /**
    * Flag to be set in validation
    */
  @volatile
  protected var useControllerService: Boolean = false

  protected def getGeomesaControllerService(context: ProcessContext): GeomesaConfigService = {
    context.getProperty(GeoMesaConfigController).asControllerService().asInstanceOf[GeomesaConfigService]
  }

  protected def getDataStoreFromParams(context: ProcessContext): DataStore =
    DataStoreFinder.getDataStore(Map(
      "zookeepers" -> context.getProperty(Zookeepers).getValue,
      "instanceId" -> context.getProperty(InstanceName).getValue,
      "tableName"  -> context.getProperty(Catalog).getValue,
      "user"       -> context.getProperty(User).getValue,
      "password"   -> context.getProperty(Password).getValue
    ))

  // Abstract
  override protected def getDataStore(context: ProcessContext): DataStore = {
    if (useControllerService) {
      getGeomesaControllerService(context).getDataStore
    } else {
      getDataStoreFromParams(context)
    }
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {

    val validationFailures = new util.ArrayList[ValidationResult]()

    useControllerService = validationContext.getProperty(GeoMesaConfigController).isSet
    val paramsSet = Seq(Zookeepers, InstanceName, User, Password, Catalog).forall(validationContext.getProperty(_).isSet)

    // require either controller-service or all of {zoo,instance,user,pw,catalog}
    if (!useControllerService && !paramsSet)
      validationFailures.add(new ValidationResult.Builder()
        .input("Use either GeoMesa Configuration Service, or specify accumulo connection parameters.")
        .build)

    // If using converters checkf or params relevant to that
    def useConverter = validationContext.getProperty(IngestModeProp).getValue == IngestMode.Converter
    if (useConverter) {
      // make sure either a sft is named or written
      val sftNameSet = validationContext.getProperty(SftName).isSet
      val sftSpecSet = validationContext.getProperty(SftSpec).isSet
      if (!sftNameSet && !sftSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a simple feature type by name or spec")
          .build)

      val convNameSet = validationContext.getProperty(ConverterName).isSet
      val convSpecSet = validationContext.getProperty(ConverterSpec).isSet
      if (!convNameSet && !convSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a converter by name or spec")
          .build
        )
    }

    validationFailures
  }

}

object PutGeoMesa {

  val GeoMesaConfigController = new PropertyDescriptor.Builder()
    .name("GeoMesa Configuration Service")
    .description("The controller service used to connect to Accumulo")
    .required(false)
    .identifiesControllerService(classOf[GeomesaConfigService])
    .build

  val Zookeepers = new PropertyDescriptor.Builder()
    .name("Zookeepers")
    .description("Zookeepers host(:port) pairs, comma separated")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val InstanceName = new PropertyDescriptor.Builder()
    .name("Instance")
    .description("Accumulo instance name")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val User = new PropertyDescriptor.Builder()
    .name("User")
    .description("Accumulo user name")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val Password = new PropertyDescriptor.Builder()
    .name("Password")
    .description("Accumulo password")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .sensitive(true)
    .build

  val Catalog = new PropertyDescriptor.Builder()
    .name("Catalog")
    .description("GeoMesa catalog table name")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build


}