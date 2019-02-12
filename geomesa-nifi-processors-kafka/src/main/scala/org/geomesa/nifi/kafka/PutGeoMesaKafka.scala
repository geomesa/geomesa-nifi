/***********************************************************************
 * Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.kafka

import java.util

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.geomesa.nifi.geo.{AbstractGeoIngestProcessor, IngestMode}
import org.geomesa.nifi.kafka.PutGeoMesaKafka._
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.{KafkaDataStoreFactoryParams => KDSP}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "kafka", "streaming", "stream", "geo", "ingest", "convert", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaKafka extends AbstractGeoIngestProcessor {

  protected override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)

    descriptors = (getPropertyDescriptors ++ KdsNifiProps).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  // Abstract
  override protected def getDataStore(context: ProcessContext): DataStore = {
    val props = KdsNifiProps.flatMap { p =>
      val value = context.getProperty(p.getName).getValue
      if (value == null) { Seq.empty } else {
        Seq(p.getName -> value)
      }
    } :+ (KDSP.ConsumerCount.getName -> 0) // only producing
    getLogger.trace(s"DataStore Properties: $props")
    DataStoreFinder.getDataStore(props.toMap.asJava)
  }
}

object PutGeoMesaKafka {

  // note: KafkaDataStoreFactory.ParameterInfo is consumer-oriented, but we want producer properties here
  val KdsGTProps = List(
    KDSP.Brokers,
    KDSP.Zookeepers,
    KDSP.ZkPath,
    KDSP.ProducerConfig,
    KDSP.TopicPartitions,
    KDSP.TopicReplication
  )

  val KdsNifiProps: List[PropertyDescriptor] = KdsGTProps.map(AbstractGeoIngestProcessor.property)
}
