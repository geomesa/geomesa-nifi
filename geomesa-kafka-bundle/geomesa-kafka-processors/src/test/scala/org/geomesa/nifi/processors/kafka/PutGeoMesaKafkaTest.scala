/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.util.{TestRunner, TestRunners}
import org.geomesa.nifi.datastore.processor.mixins.{ConvertInputProcessor, DataStoreProcessor, FeatureTypeProcessor}
import org.geomesa.nifi.datastore.processor.{PutGeoMesa, Relationships}
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.EmbeddedKafka
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PutGeoMesaKafkaTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  private lazy val kafka: EmbeddedKafka = new EmbeddedKafka()

  lazy val dsParams = Map(
    "kafka.brokers" -> kafka.brokers // note: zk-less usage
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

      val ds =
        DataStoreFinder.getDataStore((dsParams ++
            Map(KafkaDataStoreParams.Catalog.key -> catalog, KafkaDataStoreParams.ConsumerReadBack.key -> "Inf")).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
      } finally {
        ds.dispose()
      }
    }
  }

  step {
    kafka.close()
  }
}
