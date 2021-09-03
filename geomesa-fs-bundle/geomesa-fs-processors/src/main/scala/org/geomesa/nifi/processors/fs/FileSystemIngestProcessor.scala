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
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.mixins.{AbstractDataStoreProcessor, AwsDataStoreProcessor, DataStoreIngestProcessor, UserDataProcessor}
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.storage.common.utils.PartitionSchemeArgResolver
import org.locationtech.geomesa.fs.storage.common.{StorageKeys, StorageSerialization}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.opengis.feature.simple.SimpleFeatureType

abstract class FileSystemIngestProcessor
    extends AbstractDataStoreProcessor(FileSystemIngestProcessor.FileSystemProperties)
        with DataStoreIngestProcessor
        with UserDataProcessor
        with AwsDataStoreProcessor {

  override protected def configParam: GeoMesaParam[String] = FileSystemDataStoreParams.ConfigsParam

  import FileSystemIngestProcessor.PartitionSchemeParam

  private var partitionScheme: Option[String] = None

  @OnScheduled
  override def initialize(context: ProcessContext): Unit = {
    super.initialize(context)
    partitionScheme = Option(context.getProperty(PartitionSchemeParam).getValue)
  }

  override protected def loadFeatureTypeUserData(
      sft: SimpleFeatureType,
      context: ProcessContext,
      file: FlowFile): java.util.Map[String, String] = {
    val base = super.loadFeatureTypeUserData(sft, context, file)
    if (!base.containsKey(StorageKeys.SchemeKey)) {
      partitionScheme.foreach { arg =>
        logger.info(s"Adding partition scheme to ${sft.getTypeName}")
        val scheme = PartitionSchemeArgResolver.resolve(sft, arg) match {
          case Left(e) => throw new IllegalArgumentException(e)
          case Right(s) => s
        }
        logger.info(s"Loaded partition scheme: ${scheme.name}")
        base.put(StorageKeys.SchemeKey, StorageSerialization.serialize(scheme))
      }
    }
    base
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

  private val FileSystemProperties = FileSystemDataStoreService.Properties :+ PartitionSchemeParam
}
