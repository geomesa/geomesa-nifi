/***********************************************************************
 * Copyright (c) 2015-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors
package datastore

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.util.StandardValidators
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.tools.utils.PartitionSchemeArgResolver
import org.opengis.feature.simple.SimpleFeatureType

@Tags(Array("geomesa", "geo", "ingest", "convert", "hdfs", "s3", "geotools"))
@CapabilityDescription("Convert and ingest data files into a GeoMesa FileSystem Datastore")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaFileSystem
    extends AwsGeoIngestProcessor(PutGeoMesaFileSystem.FileSystemProperties, FileSystemDataStoreParams.ConfigsParam) {

  import PutGeoMesaFileSystem._

  override protected def loadSft(context: ProcessContext): SimpleFeatureType = {
    import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType
    val sft = super.loadSft(context)

    Option(context.getProperty(PartitionSchemeParam).getValue).foreach { arg =>
      logger.info(s"Adding partition scheme to ${sft.getTypeName}")
      val scheme = PartitionSchemeArgResolver.resolve(sft, arg) match {
        case Left(e) => throw new IllegalArgumentException(e)
        case Right(s) => s
      }
      sft.setScheme(scheme.name, scheme.options)
      logger.info(s"Updated SFT with partition scheme: ${scheme.name}")
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
    FileSystemDataStoreFactory.ParameterInfo.toList.map(createPropertyDescriptor) :+ PartitionSchemeParam
}
