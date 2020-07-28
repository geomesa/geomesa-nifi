/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.processor.ProcessContext
import org.geomesa.nifi.datastore.processor.AbstractGeoIngestProcessor
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams.{ProducerConfig, TopicPartitions, TopicReplication}

@Tags(Array("geomesa", "kafka", "streaming", "stream", "geo", "ingest", "convert", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
abstract class GeoMesaKafkaProcessor extends AbstractGeoIngestProcessor(GeoMesaKafkaProcessor.KafkaProperties) {
  // set consumer count to zero to disable consuming
  override protected def getDataStoreParams(context: ProcessContext): Map[String, _] =
    super.getDataStoreParams(context) ++ Map(KafkaDataStoreFactoryParams.ConsumerCount.getName -> Int.box(0))
}

object GeoMesaKafkaProcessor extends PropertyDescriptorUtils {
  // note: KafkaDataStoreFactory.ParameterInfo is consumer-oriented, but we want producer properties here
  private val KafkaProperties =
    createPropertyDescriptors(KafkaDataStoreFactory) ++
        Seq(ProducerConfig, TopicPartitions, TopicReplication).map(createPropertyDescriptor)
}
