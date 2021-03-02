/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.fs

import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.{AwsDataStoreProcessor, DataStoreIngestProcessor, DataStoreProcessor}
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.tools.utils.PartitionSchemeArgResolver
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.opengis.feature.simple.SimpleFeatureType

abstract class FileSystemIngestProcessor
    extends DataStoreProcessor(FileSystemIngestProcessor.FileSystemProperties)
        with DataStoreIngestProcessor
        with AwsDataStoreProcessor {

  override protected def configParam: GeoMesaParam[String] = FileSystemDataStoreParams.ConfigsParam

  import FileSystemIngestProcessor.PartitionSchemeParam
  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  private var partitionScheme: Option[String] = None

  @OnScheduled
  override def initialize(context: ProcessContext): Unit = {
    super.initialize(context)
    partitionScheme = Option(context.getProperty(PartitionSchemeParam).getValue)
  }

  override protected def decorate(sft: SimpleFeatureType): SimpleFeatureType = {
    partitionScheme.foreach { arg =>
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

object FileSystemIngestProcessor {

  val PartitionSchemeParam: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("PartitionScheme")
        .required(false)
        .description("A partition scheme common name or config (required for creation of new store)")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build()

  private val FileSystemProperties = FileSystemProcessor.FileSystemProperties :+ PartitionSchemeParam
}
