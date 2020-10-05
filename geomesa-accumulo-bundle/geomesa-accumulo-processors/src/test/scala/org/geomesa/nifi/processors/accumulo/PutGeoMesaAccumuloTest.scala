/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.accumulo

import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.avro.AvroReader
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.util.TestRunners
import org.geomesa.nifi.datastore.processor.AvroIngestProcessor.LenientMatch
import org.geomesa.nifi.datastore.processor.records.{GeometryEncodingLabels, RecordIngestProcessor}
import org.geomesa.nifi.datastore.processor.{AvroIngestProcessor, ConverterIngestProcessor, FeatureTypeProcessor, Relationships}
import org.geotools.data.DataStoreFinder
import org.junit.{Assert, Test}
import org.locationtech.geomesa.accumulo.MiniCluster
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.text.WKTUtils

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
      runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
      runner.setProperty(ConverterIngestProcessor.ConverterNameKey, "example-csv")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
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
      attributes.put(FeatureTypeProcessor.Attributes.SftSpecAttribute, "example")
      attributes.put(FeatureTypeProcessor.Attributes.ConverterAttribute, "example-csv-attributes")
      attributes.put("my.flowfile.attribute", "foobar")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
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
      attributes.put(FeatureTypeProcessor.Attributes.SftSpecAttribute, "example")
      attributes.put(FeatureTypeProcessor.Attributes.ConverterAttribute, "example-csv")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
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
      runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
      runner.setProperty(ConverterIngestProcessor.ConverterNameKey, "example-csv")
      val attributes = new java.util.HashMap[String, String]()
      attributes.put(FeatureTypeProcessor.Attributes.SftNameAttribute, "renamed")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
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
      runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)

      runner.enqueue(getClass.getClassLoader.getResourceAsStream("bad-example-csv.avro"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 1)
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
      runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
      runner.setProperty(AvroIngestProcessor.AvroMatchMode, LenientMatch)
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)

      runner.enqueue(getClass.getClassLoader.getResourceAsStream("bad-example-csv.avro"))
      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 2)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
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

  @Test
  def testRecordIngest(): Unit = {
    val catalog = s"${root}RecordIngest"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumuloRecord())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.CatalogParam.key, catalog)
      val service = new AvroReader()
      runner.addControllerService("avro-record-reader", service)
      runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "embedded-avro-schema")
      runner.enableControllerService(service)
      runner.setProperty(RecordIngestProcessor.Properties.RecordReader, "avro-record-reader")
      runner.setProperty(RecordIngestProcessor.Properties.FeatureIdCol, "__fid__")
      runner.setProperty(RecordIngestProcessor.Properties.GeometryCols, "*geom:Point")
      runner.setProperty(RecordIngestProcessor.Properties.GeometrySerialization, GeometryEncodingLabels.Wkb)
      runner.setProperty(RecordIngestProcessor.Properties.VisibilitiesCol, "Vis")
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.avro"))

      runner.run()
      runner.assertTransferCount(Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      Assert.assertEquals(10, sft.getAttributeCount)
      Assert.assertEquals(
        Seq("__version__", "Name", "Age", "LastSeen", "Friends", "Skills", "Lon", "Lat", "geom", "__userdata__"),
        sft.getAttributeDescriptors.asScala.map(_.getLocalName))
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
      Assert.assertEquals(
        Seq("02c42fc5e8db91d9b8165c6014a23cb0", "6a6e2089854ec6e20015d1c4857b1d9b", "dbd0cf6fc8b5d3d4c54889a493bd5d12"),
        features.map(_.getID)
      )
      Assert.assertEquals(
        Seq(
          "POINT (-100.23650360107422 23)",
          "POINT (3 -62.22999954223633)",
          "POINT (40.231998443603516 -53.235599517822266)"
        ).map(WKTUtils.read),
        features.map(_.getAttribute("geom"))
      )
      Assert.assertEquals(
        Seq(
          Collections.singletonMap("geomesa.feature.visibility", "user"),
          Collections.singletonMap("geomesa.feature.visibility", "user&admin"),
          Collections.singletonMap("geomesa.feature.visibility", "user")
        ),
        features.map(_.getUserData)
      )
    } finally {
      ds.dispose()
    }
  }
}
