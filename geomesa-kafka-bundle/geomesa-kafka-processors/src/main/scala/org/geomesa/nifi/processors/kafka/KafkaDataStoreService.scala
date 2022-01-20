/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.controller.ConfigurationContext
import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams.{ProducerConfig, TopicPartitions, TopicReplication}
import org.locationtech.geomesa.kafka.data.{KafkaDataStoreFactory, KafkaDataStoreParams}

@Tags(Array("geomesa", "geotools", "geo", "kafka"))
@CapabilityDescription("Service for connecting to GeoMesa Kafka stores")
class KafkaDataStoreService
    extends GeoMesaDataStoreService[KafkaDataStoreFactory](KafkaDataStoreService.Properties) {
  // set consumer count to zero to disable consuming
  override protected def getDataStoreParams(context: PropertyContext): Map[String, _ <: AnyRef] =
    super.getDataStoreParams(context) ++ Map(KafkaDataStoreParams.ConsumerCount.getName -> Int.box(0))
}

object KafkaDataStoreService extends PropertyDescriptorUtils {
  // note: KafkaDataStoreFactory.ParameterInfo is consumer-oriented, but we want producer properties here
  val Properties: Seq[PropertyDescriptor] =
    createPropertyDescriptors(KafkaDataStoreFactory) ++
        Seq(ProducerConfig, TopicPartitions, TopicReplication).map(createPropertyDescriptor)
}
