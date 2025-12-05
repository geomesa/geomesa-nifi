/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.kafka.nar

import org.geomesa.nifi.datastore.processor.NiFiContainer
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class KafkaNarIT extends SpecificationWithJUnit with BeforeAfterAll {

  import scala.collection.JavaConverters._

  protected val network = Network.newNetwork()

  // listener for other containers in the docker network
  val dockerNetworkBrokers = "kafka:19092"

  private val kafka =
    new KafkaContainer(KafkaNarIT.KafkaImage)
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withListener(dockerNetworkBrokers)
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka")))

  private val nifi =
    new NiFiContainer()
      .withNarByName("kafka")
      // this flow includes the default ingest flow, plus a GetGeoMesaKafkaRecord processor to read and re-ingest
      .withFlowFromClasspath("kafka-flow.json")
      .withNetwork(network)
      .dependsOn(kafka)
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("nifi")))

  lazy private val params = Map(
    KafkaDataStoreParams.Brokers.key -> kafka.getBootstrapServers,
    KafkaDataStoreParams.ConsumerReadBack.key -> "Inf"
  )

  override def beforeAll(): Unit = Startables.deepStart(kafka, nifi).get()

  override def afterAll(): Unit = CloseWithLogging(nifi, kafka)

  "Kafka NAR" should {
    "ingest data" in {
      val typeNames = NiFiContainer.DefaultIngestTypes
      WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        eventually(30, Duration(1, TimeUnit.SECONDS))(ds.getTypeNames.toSeq must containAllOf(typeNames))
        foreach(typeNames) { typeName =>
          eventually(10, Duration(1, TimeUnit.SECONDS)) {
            val features = CloseableIterator(ds.getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT)).toList
            features.length mustEqual 2362
          }
        }
      }
    }
    "read kafka records" in {
      val typeName = "gdelt-kafka-records"
      WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        eventually(45, Duration(1, TimeUnit.SECONDS)) {
          ds.getTypeNames.toSeq must contain(typeName)
          val features = CloseableIterator(ds.getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT)).toList
          features.length mustEqual 2362
        }
      }
    }
  }
}

object KafkaNarIT {
  val KafkaImage =
    DockerImageName.parse("apache/kafka-native")
      .withTag(sys.props.getOrElse("kafka.docker.tag", "3.9.1"))
}
