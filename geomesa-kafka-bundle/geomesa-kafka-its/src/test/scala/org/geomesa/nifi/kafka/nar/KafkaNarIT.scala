/*
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.nifi.kafka.nar

import org.geomesa.nifi.datastore.processor.NiFiContainer
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.KafkaContainerTest
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.runner.JUnitRunner

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class KafkaNarIT extends KafkaContainerTest {

  import scala.collection.JavaConverters._

  private var nifiContainer: NiFiContainer = _

  lazy private val params = Map(
    KafkaDataStoreParams.Brokers.key -> brokers,
    KafkaDataStoreParams.ConsumerReadBack.key -> "Inf"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    nifiContainer =
      new NiFiContainer()
          .withNarByName("kafka")
          // this flow includes the default ingest flow, plus a GetGeoMesaKafkaRecord processor to read and re-ingest
          .withFlowFromClasspath("kafka-flow.json")
          .withNetwork(network)
    nifiContainer.start()
  }

  override def afterAll(): Unit = {
    try {
      if (nifiContainer != null) {
        nifiContainer.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  "Kafka NAR" should {
    "ingest data" in {
      val typeNames = NiFiContainer.DefaultIngestTypes
      WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        eventually(30, Duration(1, TimeUnit.SECONDS))(ds.getTypeNames.toSeq must containAllOf(typeNames))
        foreach(typeNames) { typeName =>
          eventually(10, Duration(1, TimeUnit.SECONDS)) {
            val features = SelfClosingIterator(ds.getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT)).toList
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
          val features = SelfClosingIterator(ds.getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT)).toList
          features.length mustEqual 2362
        }
      }
    }
  }
}
