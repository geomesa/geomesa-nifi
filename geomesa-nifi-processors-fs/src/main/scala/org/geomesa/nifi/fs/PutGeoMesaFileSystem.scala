package org.geomesa.nifi.fs

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.fs.PutGeoMesaFileSystem._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.conf.{PartitionSchemeArgResolver, SchemeArgs}
import org.opengis.feature.simple.SimpleFeatureType

@Tags(Array("geomesa", "geo", "ingest", "convert", "hdfs", "s3", "geotools"))
@CapabilityDescription("Convert and ingest data files into a GeoMesa FileSystem Datastore")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaFileSystem extends AbstractGeoIngestProcessor(PutGeoMesaFileSystem.FileSystemProperties) {

  override protected def loadSft(context: ProcessContext): SimpleFeatureType = {
    val sft = super.loadSft(context)

    Option(context.getProperty(PartitionSchemeParam).getValue).foreach { arg =>
      logger.info(s"Adding partition scheme to ${sft.getTypeName}")
      val scheme = PartitionSchemeArgResolver.getArg(SchemeArgs(arg, sft)) match {
        case Left(e) => throw new IllegalArgumentException(e)
        case Right(s) => s
      }
      PartitionScheme.addToSft(sft, scheme)
      logger.info(s"Updated SFT with partition scheme: ${scheme.getName}")
    }

    sft
  }
}

object PutGeoMesaFileSystem {

  val PartitionSchemeParam: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("PartitionScheme")
        .required(false)
        .description("A partition scheme common name or config (required for creation of new store)")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build()

  private val FileSystemProperties =
    FileSystemDataStoreFactory.ParameterInfo.toList.map(AbstractGeoIngestProcessor.property) :+ PartitionSchemeParam
}
