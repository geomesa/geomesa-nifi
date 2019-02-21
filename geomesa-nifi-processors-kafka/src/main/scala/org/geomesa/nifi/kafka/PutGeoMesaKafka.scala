/***********************************************************************
 * Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.kafka

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.processor.ProcessContext
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor
import org.geotools.data.DataStore
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams

@Tags(Array("geomesa", "kafka", "streaming", "stream", "geo", "ingest", "convert", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaKafka extends AbstractGeoIngestProcessor(PutGeoMesaKafka.KafkaProperties) {

  // set consumer count to zero to disable consuming
  override protected def loadDataStore(context: ProcessContext, static: Map[String, _]): DataStore =
    super.loadDataStore(context, static ++ Map(KafkaDataStoreFactoryParams.ConsumerCount.getName -> Int.box(0)))
}

object PutGeoMesaKafka {

  import KafkaDataStoreFactoryParams._

  // note: KafkaDataStoreFactory.ParameterInfo is consumer-oriented, but we want producer properties here
  private val KafkaProperties = {
    val params = Seq(Brokers, Zookeepers, ZkPath, ProducerConfig, TopicPartitions, TopicReplication)
    params.map(AbstractGeoIngestProcessor.property)
  }
}
