package org.locationtech.geomesa.nifi


import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.PropertyDescriptor.Builder
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.{DataStoreFinder, DataStore}
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.locationtech.geomesa.nifi.GeoMesaIngest._
import org.locationtech.geomesa.nifi.GeomesaConfigControllerService

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


@Tags(Array("geomesa", "geo", "ingest"))
abstract class AbstractGeoMesa extends AbstractProcessor {

  @volatile
  protected var dataStore: DataStore = null

  protected var descriptors: java.util.List[PropertyDescriptor] = null
  protected var relationships: java.util.Set[Relationship] = null

  override def getRelationships = relationships
  override def getSupportedPropertyDescriptors = descriptors

  /*
  protected def getDataStore(context: ProcessContext): DataStore = DataStoreFinder.getDataStore(Map(
    "zookeepers" -> context.getProperty(Zookeepers).getValue,
    "instanceId" -> context.getProperty(InstanceName).getValue,
    "tableName"  -> context.getProperty(Catalog).getValue,
    "user"       -> context.getProperty(User).getValue,
    "password"   -> context.getProperty(Password).getValue
  ))
  */

}
object AbstractGeoMesa {
    val GeoMesaConfigController = new PropertyDescriptor.Builder()
    .name("GeoMesaConfigurationControllerService")
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

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
}