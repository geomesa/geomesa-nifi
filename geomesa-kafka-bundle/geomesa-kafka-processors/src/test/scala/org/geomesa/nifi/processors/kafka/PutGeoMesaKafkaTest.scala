/***********************************************************************
 * Copyright (c) 2015-2024 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import org.apache.nifi.util.{TestRunner, TestRunners}
import org.geomesa.nifi.datastore.processor.mixins.{ConvertInputProcessor, DataStoreProcessor, FeatureTypeProcessor}
import org.geomesa.nifi.datastore.processor.{PutGeoMesa, Relationships}
import org.geotools.api.data.DataStoreFinder
import org.geotools.api.feature.simple.SimpleFeature
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.KafkaContainerTest
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
class PutGeoMesaKafkaTest extends KafkaContainerTest {

  import scala.collection.JavaConverters._

  sequential

  lazy val dsParams = Map(
    "kafka.brokers" -> brokers // note: zk-less usage
  )

  // we use class name to prevent spillage between unit tests
  lazy val root = getClass.getSimpleName

  def configureKafkaService(runner: TestRunner, catalog: String): Unit = {
    val service = new KafkaDataStoreService()
    runner.addControllerService("data-store", service)
    dsParams.foreach { case (k, v) => runner.setProperty(service, k, v) }
    runner.setProperty(service, KafkaDataStoreParams.Catalog.key, catalog)
    runner.enableControllerService(service)
    runner.setProperty(DataStoreProcessor.Properties.DataStoreService, "data-store")
  }

  "kafka processor" should {
    "ingest using converters" in {
      val catalog = s"${root}Ingest"
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configureKafkaService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val readback =
        Map(
          KafkaDataStoreParams.Catalog.key -> catalog,
          KafkaDataStoreParams.ConsumerReadBack.key -> "Inf",
          KafkaDataStoreParams.LazyLoad.key -> "false"
        )
      val ds = DataStoreFinder.getDataStore((dsParams ++ readback).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)

        def checkFeatures(): List[SimpleFeature] =
          SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList

        eventually(40, 100.millis)(checkFeatures() must haveLength(3))
      } finally {
        ds.dispose()
      }
    }
  }
}
