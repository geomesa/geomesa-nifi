/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.accumulo

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.util.TestRunners
import org.geomesa.nifi.datastore.processor.{AbstractGeoIngestProcessor, AvroIngestProcessor, ConverterIngestProcessor}
import org.geotools.data.DataStoreFinder
import org.junit.{Assert, Test}
import org.locationtech.geomesa.accumulo.MiniCluster
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.collection.SelfClosingIterator

class PutGeoMesaAccumuloTest extends LazyLogging {

  import scala.collection.JavaConverters._

  // we use class name to prevent spillage between unit tests
  lazy val root = s"${MiniCluster.namespace}.${getClass.getSimpleName}"

  // note the table needs to be different to prevent tests from conflicting with each other
  lazy val dsParams: Map[String, String] = Map(
    AccumuloDataStoreParams.InstanceIdParam.key -> MiniCluster.cluster.getInstanceName,
    AccumuloDataStoreParams.ZookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
    AccumuloDataStoreParams.UserParam.key       -> MiniCluster.Users.root.name,
    AccumuloDataStoreParams.PasswordParam.key   -> MiniCluster.Users.root.password
  )

  @Test
  def testIngest(): Unit = {
    val catalog = s"${root}Ingest"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(AbstractGeoIngestProcessor.Properties.SftNameKey, "example")
      runner.setProperty(ConverterIngestProcessor.ConverterNameKey, "example-csv")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testIngestConvertAttributes(): Unit = {
    val catalog = s"${root}IngestConvertAttributes"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(ConverterIngestProcessor.ConvertFlowFileAttributes, "true")
      val attributes = new java.util.HashMap[String, String]()
      attributes.put(AbstractGeoIngestProcessor.Attributes.SftSpecAttribute, "example")
      attributes.put(AbstractGeoIngestProcessor.Attributes.ConverterAttribute, "example-csv-attributes")
      attributes.put("my.flowfile.attribute", "foobar")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
      Assert.assertEquals(Seq("foobar2", "foobar3", "foobar4"), features.map(_.getID).sorted)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testIngestConfigureAttributes(): Unit = {
    val catalog = s"${root}IngestConfigureAttributes"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      val attributes = new java.util.HashMap[String, String]()
      attributes.put(AbstractGeoIngestProcessor.Attributes.SftSpecAttribute, "example")
      attributes.put(AbstractGeoIngestProcessor.Attributes.ConverterAttribute, "example-csv")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testIngestConfigureAttributeOverride(): Unit = {
    val catalog = s"${root}IngestConfigureAttributeOverride"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(AbstractGeoIngestProcessor.Properties.SftNameKey, "example")
      runner.setProperty(ConverterIngestProcessor.ConverterNameKey, "example-csv")
      val attributes = new java.util.HashMap[String, String]()
      attributes.put(AbstractGeoIngestProcessor.Attributes.SftNameAttribute, "renamed")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("renamed")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("renamed").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testAvroIngest(): Unit = {
    val catalog = s"${root}AvroIngest"
    val runner = TestRunners.newTestRunner(new AvroToPutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(AbstractGeoIngestProcessor.Properties.SftNameKey, "example")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)

      runner.enqueue(getClass.getClassLoader.getResourceAsStream("bad-example-csv.avro"))
      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 1)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testAvroIngestByName(): Unit = {
    val catalog = s"${root}AvroIngestByName"
    val runner = TestRunners.newTestRunner(new AvroToPutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      runner.setProperty(AbstractGeoIngestProcessor.Properties.SftNameKey, "example")
      runner.setProperty(AvroIngestProcessor.AvroMatchMode, "lenient")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)

      runner.enqueue(getClass.getClassLoader.getResourceAsStream("bad-example-csv.avro"))
      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 2)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(6, features.length)
    } finally {
      ds.dispose()
    }
  }
}
