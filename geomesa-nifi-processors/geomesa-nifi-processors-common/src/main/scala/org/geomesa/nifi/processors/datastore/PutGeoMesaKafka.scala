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
import org.apache.nifi.processor.ProcessContext
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams

@Tags(Array("geomesa", "kafka", "streaming", "stream", "geo", "ingest", "convert", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaKafka extends AbstractGeoIngestProcessor(PutGeoMesaKafka.KafkaProperties) {
  // set consumer count to zero to disable consuming
  override protected def getDataStoreParams(context: ProcessContext): Map[String, _] =
    super.getDataStoreParams(context) ++ Map(KafkaDataStoreFactoryParams.ConsumerCount.getName -> Int.box(0))
}

object PutGeoMesaKafka {

  import KafkaDataStoreFactoryParams._

  // note: KafkaDataStoreFactory.ParameterInfo is consumer-oriented, but we want producer properties here
  private val KafkaProperties =
    Seq(Brokers, Zookeepers, ZkPath, ProducerConfig, TopicPartitions, TopicReplication).map(createPropertyDescriptor)
}
