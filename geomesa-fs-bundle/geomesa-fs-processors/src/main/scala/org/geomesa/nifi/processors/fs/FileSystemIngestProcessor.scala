/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.fs

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.mixins.BaseProcessor.FeatureTypeDecorator
import org.geomesa.nifi.datastore.processor.mixins.{AwsDataStoreProcessor, DataStoreIngestProcessor, DataStoreProcessor}
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.tools.utils.PartitionSchemeArgResolver
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.opengis.feature.simple.SimpleFeatureType

abstract class FileSystemIngestProcessor
    extends DataStoreProcessor(FileSystemProcessor.FileSystemProperties)
        with DataStoreIngestProcessor
        with AwsDataStoreProcessor {

  import FileSystemIngestProcessor.PartitionSchemeParam
  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  override protected def getTertiaryProperties: Seq[PropertyDescriptor] =
    super.getTertiaryProperties ++ Seq(PartitionSchemeParam)

  override protected def configParam: GeoMesaParam[String] = FileSystemDataStoreParams.ConfigsParam

  override protected val decorator: Option[FeatureTypeDecorator] = {
    val decorator: FeatureTypeDecorator = new FeatureTypeDecorator() {
      override val properties: Seq[PropertyDescriptor] = Seq(PartitionSchemeParam)
      override def decorate(sft: SimpleFeatureType, properties: Map[PropertyDescriptor, String]): SimpleFeatureType = {
        properties.get(PartitionSchemeParam).foreach { arg =>
          logger.info(s"Adding partition scheme to ${sft.getTypeName}")
          val scheme = PartitionSchemeArgResolver.resolve(sft, arg) match {
            case Left(e) => throw new IllegalArgumentException(e)
            case Right(s) => s
          }
          sft.setScheme(scheme.name, scheme.options)
          logger.info(s"Updated sft with partition scheme: ${scheme.name}")
        }
        sft
      }
    }
    Some(decorator)
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
}
