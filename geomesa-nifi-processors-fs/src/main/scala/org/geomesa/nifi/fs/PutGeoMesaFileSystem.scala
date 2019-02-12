package org.geomesa.nifi.fs

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.{ProcessContext, ProcessorInitializationContext}
import org.geomesa.nifi.fs.PutGeoMesaFileSystem._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.conf.{PartitionSchemeArgResolver, SchemeArgs}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "convert", "hdfs", "s3", "geotools"))
@CapabilityDescription("Convert and ingest data files into a GeoMesa FileSystem Datastore")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaFileSystem extends AbstractGeoIngestProcessor {

  protected override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)
    descriptors = (getPropertyDescriptors ++ FSNifiProps).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override protected def getSft(context: ProcessContext): SimpleFeatureType = {
    val sft = super.getSft(context)

    if (context.getProperty(PartitionSchemeParam).getValue != null) {
      getLogger.info(s"Adding partition scheme to ${sft.getTypeName}")
      val psString = context.getProperty(PartitionSchemeParam).getValue
      val scheme = PartitionSchemeArgResolver.getArg(SchemeArgs(psString, sft)) match {
        case Left(e) => throw new IllegalArgumentException(e)
        case Right(s) => s
      }
      PartitionScheme.addToSft(sft, scheme)
      getLogger.info(s"Updated SFT with partition scheme: ${scheme.getName()}")
    }

    sft
  }

  override protected def getDataStore(context: ProcessContext): DataStore = {
    val props = FSNifiProps.flatMap { p =>
      val value = context.getProperty(p.getName).getValue
      if (value == null) { Seq.empty } else {
        Seq(p.getName -> value)
      }
    }
    getLogger.trace(s"DataStore Properties: $props")
    DataStoreFinder.getDataStore(props.toMap.asJava)
  }
}

object PutGeoMesaFileSystem {

  val PartitionSchemeParam: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("PartitionScheme")
    .description("A partition scheme common name or config (required for creation of new store)")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build()

  val FSNifiProps: List[PropertyDescriptor] =
    FileSystemDataStoreFactory.ParameterInfo.toList.map(AbstractGeoIngestProcessor.property) :+ PartitionSchemeParam
}
