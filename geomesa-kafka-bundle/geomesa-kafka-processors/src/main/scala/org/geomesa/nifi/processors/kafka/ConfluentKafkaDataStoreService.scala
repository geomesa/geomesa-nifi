/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.kafka.confluent.ConfluentKafkaDataStoreFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams

@Tags(Array("geomesa", "geotools", "geo", "kafka", "confluent", "schema-registry", "avro"))
@CapabilityDescription("Service for connecting to GeoMesa Confluent Kafka stores")
class ConfluentKafkaDataStoreService
    extends GeoMesaDataStoreService[ConfluentKafkaDataStoreFactory](ConfluentKafkaDataStoreService.Properties) {
  // set consumer count to zero to disable consuming
  override protected def getDataStoreParams(context: PropertyContext): Map[String, _ <: AnyRef] =
    super.getDataStoreParams(context) ++ Map(KafkaDataStoreParams.ConsumerCount.getName -> Int.box(0))
}

object ConfluentKafkaDataStoreService extends PropertyDescriptorUtils {

  private val params =
    Seq(
      KafkaDataStoreParams.Brokers,
      ConfluentKafkaDataStoreFactory.SchemaRegistryUrl,
      ConfluentKafkaDataStoreFactory.SchemaOverrides,
      KafkaDataStoreParams.ProducerConfig,
      KafkaDataStoreParams.TopicPartitions,
      KafkaDataStoreParams.TopicReplication,
      KafkaDataStoreParams.ClearOnStart,
    )

  val Properties: Seq[PropertyDescriptor] = params.map(createPropertyDescriptor)
}
