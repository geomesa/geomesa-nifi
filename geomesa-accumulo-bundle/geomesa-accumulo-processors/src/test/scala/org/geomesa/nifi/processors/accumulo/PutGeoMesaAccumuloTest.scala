/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.accumulo

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.security.Authorizations
import org.apache.nifi.avro.AvroReader
import org.apache.nifi.json.JsonTreeReader
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.apache.nifi.schema.inference.SchemaInferenceUtil
import org.apache.nifi.util.{TestRunner, TestRunners}
import org.geomesa.nifi.datastore.processor._
import org.geomesa.nifi.datastore.processor.mixins.{ConvertInputProcessor, DataStoreIngestProcessor, DataStoreProcessor, FeatureTypeProcessor}
import org.geomesa.nifi.datastore.processor.records.Properties
import org.geomesa.testcontainers.AccumuloContainer
import org.geotools.api.data.{DataStoreFinder, Transaction}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.io.AvroDataFileWriter
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypeLoader, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.matcher.MatchResult
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAll

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Date}

class PutGeoMesaAccumuloTest extends SpecificationWithJUnit with BeforeAll with LazyLogging {

  import scala.collection.JavaConverters._

  private val catalogCounter = new AtomicInteger(0)

  // note the table needs to be different to prevent tests from conflicting with each other
  lazy val dsParams: Map[String, String] = Map(
    AccumuloDataStoreParams.InstanceNameParam.key -> AccumuloContainer.getInstance().getInstanceName,
    AccumuloDataStoreParams.ZookeepersParam.key   -> AccumuloContainer.getInstance().getZookeepers,
    AccumuloDataStoreParams.UserParam.key         -> AccumuloContainer.getInstance().getUsername,
    AccumuloDataStoreParams.PasswordParam.key     -> AccumuloContainer.getInstance().getPassword
  )

  def configureAccumuloService(runner: TestRunner, catalog: String): AccumuloDataStoreService = {
    val service = new AccumuloDataStoreService()
    runner.addControllerService("data-store", service)
    dsParams.foreach { case (k, v) => runner.setProperty(service, k, v) }
    runner.setProperty(service, AccumuloDataStoreParams.CatalogParam.key, catalog)
    runner.enableControllerService(service)
    runner.setProperty(DataStoreProcessor.Properties.DataStoreService, "data-store")
    service
  }

  // we use class name to prevent spillage between unit tests
  def nextCatalog(): String = s"gm.${getClass.getSimpleName}${catalogCounter.getAndIncrement()}"

  override def beforeAll(): Unit = {
    WithClose(AccumuloContainer.getInstance().client()) { client =>
      val secOps = client.securityOperations()
      secOps.changeUserAuthorizations("root", new Authorizations("admin", "user"))
    }
  }

  "PutGeoMesa" should {
    "ingest to Accumulo" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
      } finally {
        ds.dispose()
      }
    }

    "SpecValidation" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.SftSpec,
          "fid:Int,name:String,age:Int,dtg:Date,geom:Point:srid=4326")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
        runner.assertNotValid()
        runner.setProperty(FeatureTypeProcessor.Properties.FeatureNameOverride, "example")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
      } finally {
        ds.dispose()
      }
    }

    "IngestConvertAttributes" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(ConvertInputProcessor.Properties.ConvertFlowFileAttributes, "true")
        val attributes = new java.util.HashMap[String, String]()
        attributes.put(FeatureTypeProcessor.Attributes.SftSpecAttribute, "example")
        attributes.put(ConvertInputProcessor.Attributes.ConverterAttribute, "example-csv-attributes")
        attributes.put("my.flowfile.attribute", "foobar")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
        features.map(_.getID).sorted mustEqual Seq("foobar2", "foobar3", "foobar4")
      } finally {
        ds.dispose()
      }
    }

    "IngestConfigureAttributes" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        val attributes = new java.util.HashMap[String, String]()
        attributes.put(FeatureTypeProcessor.Attributes.SftSpecAttribute, "example")
        attributes.put(ConvertInputProcessor.Attributes.ConverterAttribute, "example-csv")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"), attributes)
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
      } finally {
        ds.dispose()
      }
    }

    "IngestConfigureAttributeOverride" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
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
      ds must not(beNull)
      try {
        val sft = ds.getSchema("renamed")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("renamed").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
      } finally {
        ds.dispose()
      }
    }

    "AvroIngest" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new AvroToPutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.FeatureNameOverride, "example")
        runner.setProperty(DataStoreIngestProcessor.Properties.SchemaCompatibilityMode, CompatibilityMode.Exact.toString)
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
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
      } finally {
        ds.dispose()
      }
    }

    "AvroIngestByName" in {
      val catalog = nextCatalog()

      // Let's make a new Avro file
      val sft2 = SimpleFeatureTypes.createType("test2", "name:String,*geom:Point:srid=4326,dtg:Date")
      val pt = WKTUtils.read("POINT(1.2 3.4)")
      val date = new Date()

      val baos = new ByteArrayOutputStream()
      WithClose(new AvroDataFileWriter(baos, sft2)) { writer =>
        val sf = new ScalaSimpleFeature(sft2, "sf2-record", Array("Ray", pt, date))
        writer.append(sf)
        writer.flush()
      }

      val is = new ByteArrayInputStream(baos.toByteArray)

      val runner = TestRunners.newTestRunner(new AvroToPutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(DataStoreIngestProcessor.Properties.SchemaCompatibilityMode, CompatibilityMode.Existing.toString)

        runner.enqueue(is)
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore(
        (dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        sft.getAttributeCount mustEqual 5
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        features must haveLength(1)
        logger.debug(features.mkString(";"))
        features.head.getAttributes.asScala mustEqual Seq(null, "Ray", null, date, pt)
      } finally {
        ds.dispose()
      }
    }

    "AvroIngestWithNameOverride" in {
      val catalog = nextCatalog()

      // Let's make a new Avro file
      val sft2 = SimpleFeatureTypes.createType("test2", "lastseen:Date,newField:Double,age:Int,name:String,*geom:Point:srid=4326")

      val baos = new ByteArrayOutputStream()
      val writer = new AvroDataFileWriter(baos, sft2)
      val sf = new ScalaSimpleFeature(sft2, "sf2-record", Array(new Date(), Double.box(2.34), Int.box(34), "Ray", WKTUtils.read("POINT(1.2 3.4)")))
      sf.getUserData().put("geomesa.feature.visibility", "admin")
      writer.append(sf)
      writer.flush()
      writer.close()

      val is = new ByteArrayInputStream(baos.toByteArray)

      val runner = TestRunners.newTestRunner(new AvroToPutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.FeatureNameOverride, "example")
        runner.setProperty(DataStoreIngestProcessor.Properties.SchemaCompatibilityMode, CompatibilityMode.Existing.toString)
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)

        runner.enqueue(getClass.getClassLoader.getResourceAsStream("bad-example-csv.avro"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 2)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)

        runner.enqueue(is)
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 3)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore(
        (dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog) +
            (AccumuloDataStoreParams.AuthsParam.key -> "admin")).asJava
      )
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        val ray = features.find(sf => sf.getAttribute("name") == "Ray")
        ray.map(!_.getUserData.isEmpty).get must beTrue
        logger.debug(features.mkString(";"))
        features must haveLength(7)
      } finally {
        ds.dispose()
      }
    }

    "AvroIngestWithSchema" in {
      val catalog = nextCatalog()
      val spec =
        """geomesa.sfts.example = {
          |  attributes = [
          |    { name = "name", type = "String", index = true }
          |    { name = "age",  type = "Int"                  }
          |    { name = "dtg",  type = "Date"                 }
          |    { name = "geom", type = "Point", srid = 4326   }
          |  ]
          |}
          |""".stripMargin
      val runner = TestRunners.newTestRunner(new AvroToPutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.FeatureNameOverride, "example")
        runner.setProperty(DataStoreIngestProcessor.Properties.SchemaCompatibilityMode, CompatibilityMode.Existing.toString)
        runner.setProperty(FeatureTypeProcessor.Properties.SftSpec, spec)
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
        val attributes = features.head.getFeatureType.getAttributeDescriptors.asScala.map(_.getLocalName)
        attributes mustEqual Seq("name", "age", "dtg", "geom")
      } finally {
        ds.dispose()
      }
    }

    "RecordIngest" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesaRecord())
      try {
        configureAccumuloService(runner, catalog)
        val service = new AvroReader()
        runner.addControllerService("avro-record-reader", service)
        runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "embedded-avro-schema")
        runner.enableControllerService(service)
        runner.setProperty(Properties.RecordReader, "avro-record-reader")
        runner.setProperty(Properties.FeatureIdCol, "__fid__")
        runner.setProperty(Properties.GeometryCols, "*geom:Point")
        runner.setProperty(Properties.GeometrySerializationDefaultWkt, "WKB")
        runner.setProperty(Properties.VisibilitiesCol, "Vis")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.avro"))

        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        sft.getAttributeCount mustEqual 10
        sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual
            Seq("__version__", "Name", "Age", "LastSeen", "Friends", "Skills", "Lon", "Lat", "geom", "__userdata__")
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
        logger.debug(features.mkString(";"))
        features must haveLength(3)
        features.map(_.getID) mustEqual
            Seq("02c42fc5e8db91d9b8165c6014a23cb0", "6a6e2089854ec6e20015d1c4857b1d9b", "dbd0cf6fc8b5d3d4c54889a493bd5d12")
        features.map(_.getAttribute("geom")) mustEqual
            Seq(
              "POINT (-100.23650360107422 23)",
              "POINT (3 -62.22999954223633)",
              "POINT (40.231998443603516 -53.235599517822266)"
            ).map(WKTUtils.read)
        features.map(_.getUserData) mustEqual
            Seq(
              Collections.singletonMap("geomesa.feature.visibility", "user"),
              Collections.singletonMap("geomesa.feature.visibility", "user&admin"),
              Collections.singletonMap("geomesa.feature.visibility", "user")
            )
      } finally {
        ds.dispose()
      }
    }

    "RecordIngestFlowFileAttributes" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesaRecord())
      try {
        configureAccumuloService(runner, catalog)
        val service = new AvroReader()
        runner.addControllerService("avro-record-reader", service)
        runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "embedded-avro-schema")
        runner.enableControllerService(service)
        runner.setProperty(Properties.RecordReader, "avro-record-reader")
        runner.setProperty(Properties.TypeName, "${type-name}")
        runner.setProperty(Properties.FeatureIdCol, "${id-col}")
        runner.setProperty(Properties.GeometryCols, "${geom-cols}")
        runner.setProperty(Properties.GeometrySerializationDefaultWkt, "WKB")
        runner.setProperty(Properties.VisibilitiesCol, "${vis-col}")
        val attributes = new java.util.HashMap[String, String]()
        attributes.put("type-name", "attributes")
        attributes.put("id-col", "__fid__")
        attributes.put("geom-cols", "*geom:Point")
        attributes.put("vis-col", "Vis")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.avro"), attributes)

        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("attributes")
        sft must not(beNull)
        sft.getAttributeCount mustEqual 10
        sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual
            Seq("__version__", "Name", "Age", "LastSeen", "Friends", "Skills", "Lon", "Lat", "geom", "__userdata__")
        val features = CloseableIterator(ds.getFeatureSource("attributes").getFeatures.features()).toList.sortBy(_.getID)
        logger.debug(features.mkString(";"))
        features must haveLength(3)
        features.map(_.getID) mustEqual
            Seq("02c42fc5e8db91d9b8165c6014a23cb0", "6a6e2089854ec6e20015d1c4857b1d9b", "dbd0cf6fc8b5d3d4c54889a493bd5d12")
        features.map(_.getAttribute("geom")) mustEqual
            Seq(
              "POINT (-100.23650360107422 23)",
              "POINT (3 -62.22999954223633)",
              "POINT (40.231998443603516 -53.235599517822266)"
            ).map(WKTUtils.read)
        features.map(_.getUserData) mustEqual
            Seq(
              Collections.singletonMap("geomesa.feature.visibility", "user"),
              Collections.singletonMap("geomesa.feature.visibility", "user&admin"),
              Collections.singletonMap("geomesa.feature.visibility", "user")
            )
      } finally {
        ds.dispose()
      }
    }

    "RecordIngestFidAttribute" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesaRecord())
      try {
        configureAccumuloService(runner, catalog)
        val service = new JsonTreeReader()
        runner.addControllerService("json-record-reader", service)
        runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA)
        runner.enableControllerService(service)
        runner.setProperty(Properties.RecordReader, "json-record-reader")
        runner.setProperty(Properties.FeatureIdCol, "fid")
        runner.setProperty(Properties.FeatureIdIsAttribute, "true")
        runner.setProperty(Properties.GeometryCols, "*geom:Point")
        runner.setProperty(Properties.TypeName, "example")
        val record = """{"fid":"23623","name":"Harry Potter","geom":"POINT(1 1)"}"""
        runner.enqueue(new ByteArrayInputStream(record.getBytes(StandardCharsets.UTF_8)))

        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        sft.getAttributeCount mustEqual 3
        sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("fid", "name", "geom")
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
        logger.debug(features.mkString(";"))
        features must haveLength(1)
        features.head.getID mustEqual "23623"
        features.head.getAttribute("name") mustEqual "Harry Potter"
        features.head.getAttribute("geom") mustEqual WKTUtils.read("POINT(1 1)")
      } finally {
        ds.dispose()
      }
    }

    "UpdateIngest" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesa())

      def checkResults(ids: Seq[String], names: Seq[String], dates: Seq[Date], geoms: Seq[String]): MatchResult[Any] = {
        val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
        ds must not(beNull)
        try {
          val sft = ds.getSchema("example")
          sft must not(beNull)
          val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
          logger.debug(features.mkString(";"))
          features must haveLength(3)
          features.map(_.getID) mustEqual ids
          features.map(_.getAttribute("name")) mustEqual names
          features.map(_.getAttribute("dtg")) mustEqual dates
          features.map(_.getAttribute("geom").toString) mustEqual geoms
        } finally {
          ds.dispose()
        }
      }

      val df = new SimpleDateFormat("yyyy-MM-dd")
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
        checkResults(
          Seq("23623", "26236", "3233"),
          Seq("Harry", "Hermione", "Severus"),
          Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
          Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
        )
        runner.setProperty(DataStoreIngestProcessor.Properties.WriteMode, DataStoreIngestProcessor.ModifyMode)
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-update.csv"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 2)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
        checkResults(
          Seq("23623", "26236", "3233"),
          Seq("Harry Potter", "Hermione Granger", "Severus Snape"),
          Seq("2016-05-06", "2016-06-07", "2016-10-23").map(df.parse),
          Seq("POINT (-100.2365 33)", "POINT (40.232 -43.2356)", "POINT (3 -52.23)")
        )
        // verify update by attribute
        runner.setProperty(DataStoreIngestProcessor.Properties.ModifyAttribute, "age")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-update-2.csv"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 3)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
        checkResults(
          Seq("23624", "26236", "3233"),
          Seq("Harry", "Hermione Granger", "Severus Snape"),
          Seq("2016-05-06", "2016-06-07", "2016-10-23").map(df.parse),
          Seq("POINT (-100.2365 33)", "POINT (40.232 -43.2356)", "POINT (3 -52.23)")
        )
      } finally {
        runner.shutdown()
      }
    }

    "AppendThenUpdateIngest" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesa())

      def checkResults(ages: Seq[Int], dates: Seq[Date], geoms: Seq[String]): MatchResult[Any] = {
        val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
        ds must not(beNull)
        try {
          val sft = ds.getSchema("example")
          sft must not(beNull)
          val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
          logger.debug(features.mkString(";"))
          features must haveLength(3)
          features.map(_.getID) mustEqual Seq("23623", "26236", "3233")
          features.map(_.getAttribute("name")) mustEqual Seq("Harry", "Hermione", "Severus")
          features.map(_.getAttribute("age")) mustEqual ages
          features.map(_.getAttribute("dtg")) mustEqual dates
          features.map(_.getAttribute("geom").toString) mustEqual geoms
        } finally {
          ds.dispose()
        }
      }

      val df = new SimpleDateFormat("yyyy-MM-dd")
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
        runner.setProperty(DataStoreIngestProcessor.Properties.WriteMode, "${geomesa.write.mode}")
        runner.setProperty(DataStoreIngestProcessor.Properties.ModifyAttribute, "${geomesa.update.attribute}")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
        checkResults(
          Seq(20, 25, 30),
          Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
          Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
        )

        runner.enqueue(
          getClass.getClassLoader.getResourceAsStream("example-update-3.csv"),
          Map("geomesa.write.mode" -> "modify", "geomesa.update.attribute" -> "name").asJava)
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 2)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
        checkResults(
          Seq(21, 26, 31),
          Seq("2016-05-06", "2016-06-07", "2016-10-23").map(df.parse),
          Seq("POINT (-100.2365 24)", "POINT (40.232 -52.2356)", "POINT (3 -61.23)")
        )
      } finally {
        runner.shutdown()
      }
    }

    "AppendThenUpdateIngestWithCaching" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesa())

      def checkResults(ages: Seq[Int], dates: Seq[Date], geoms: Seq[String]): MatchResult[Any] = {
        val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
        ds must not(beNull)
        try {
          val sft = ds.getSchema("example")
          sft must not(beNull)
          val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
          logger.debug(features.mkString(";"))
          features must haveLength(3)
          features.map(_.getID) mustEqual Seq("23623", "26236", "3233")
          features.map(_.getAttribute("name")) mustEqual Seq("Harry", "Hermione", "Severus")
          features.map(_.getAttribute("age")) mustEqual ages
          features.map(_.getAttribute("dtg")) mustEqual dates
          features.map(_.getAttribute("geom").toString) mustEqual geoms
        } finally {
          ds.dispose()
        }
      }

      val df = new SimpleDateFormat("yyyy-MM-dd")
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
        runner.setProperty(DataStoreIngestProcessor.Properties.WriteMode, "${geomesa.write.mode}")
        runner.setProperty(DataStoreIngestProcessor.Properties.ModifyAttribute, "${geomesa.update.attribute}")
        runner.setProperty(DataStoreIngestProcessor.Properties.FeatureWriterCaching, "true")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
        runner.enqueue(
          getClass.getClassLoader.getResourceAsStream("example-update-3.csv"),
          Map("geomesa.write.mode" -> "modify", "geomesa.update.attribute" -> "name").asJava)
        runner.run(2)
        runner.assertTransferCount(Relationships.SuccessRelationship, 2)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }
      checkResults(
        Seq(21, 26, 31),
        Seq("2016-05-06", "2016-06-07", "2016-10-23").map(df.parse),
        Seq("POINT (-100.2365 24)", "POINT (40.232 -52.2356)", "POINT (3 -61.23)")
      )
    }

    "UpdateRecord" in {
      val catalog = nextCatalog()
      val df = new SimpleDateFormat("yyyy-MM-dd")
      val runner = TestRunners.newTestRunner(new UpdateGeoMesaRecord())
      try {
        val params = dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          // create the sft and ingest 3 features
          val sft = SimpleFeatureTypeLoader.sftForName("example").get
          ds.createSchema(sft)
          val converterConf = ConverterConfigLoader.configForName("example-csv")
          val features = WithClose(SimpleFeatureConverter(sft, converterConf.get)) { converter =>
            WithClose(converter.process(getClass.getClassLoader.getResourceAsStream("example.csv")))(_.toList)
          }
          WithClose(ds.getFeatureWriterAppend("example", Transaction.AUTO_COMMIT)) { writer =>
            features.foreach(FeatureUtils.write(writer, _))
          }

          def checkResults(ids: Seq[String], names: Seq[String], dates: Seq[Date], geoms: Seq[String]): MatchResult[Any] = {
            val results = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList.sortBy(_.getID)
            logger.debug(results.mkString(";"))
            results must haveLength(3)
            results.map(_.getID) mustEqual ids
            results.map(_.getAttribute("name")) mustEqual names
            results.map(_.getAttribute("dtg")) mustEqual dates
            results.map(_.getAttribute("geom").toString) mustEqual geoms
          }

          // verify initial write
          checkResults(
            Seq("23623", "26236", "3233"),
            Seq("Harry", "Hermione", "Severus"),
            Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
            Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
          )

          // configure the processor to use json
          val service = new JsonTreeReader()
          runner.addControllerService("json-record-reader", service)
          runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA)
          runner.enableControllerService(service)
          configureAccumuloService(runner, catalog)
          runner.setProperty(Properties.RecordReader, "json-record-reader")
          runner.setProperty(Properties.TypeName, "${type-name}")
          runner.setProperty(Properties.FeatureIdCol, "${id-col}")
          runner.setProperty(UpdateGeoMesaRecord.Properties.LookupCol, "${id-col}")

          // update one name
          runner.enqueue(
            new ByteArrayInputStream("""{"fid":"23623","name":"Harry Potter"}""".getBytes(StandardCharsets.UTF_8)),
            Map("type-name"-> "example", "id-col" -> "fid").asJava)
          runner.run()
          runner.assertTransferCount(Relationships.SuccessRelationship, 1)
          runner.assertTransferCount(Relationships.FailureRelationship, 0)
          checkResults(
            Seq("23623", "26236", "3233"),
            Seq("Harry Potter", "Hermione", "Severus"),
            Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
            Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
          )

          // update the name and feature id based on matching on age
          runner.setProperty(UpdateGeoMesaRecord.Properties.LookupCol, "${match-col}")
          runner.enqueue(
            new ByteArrayInputStream("""{"fid":"26237","name":"Hermione Granger","age":25}""".getBytes(StandardCharsets.UTF_8)),
            Map("type-name"-> "example", "id-col" -> "fid", "match-col" -> "age").asJava)
          runner.run()
          runner.assertTransferCount(Relationships.SuccessRelationship, 2)
          runner.assertTransferCount(Relationships.FailureRelationship, 0)
          checkResults(
            Seq("23623", "26237", "3233"),
            Seq("Harry Potter", "Hermione Granger", "Severus"),
            Seq("2015-05-06", "2015-06-07", "2015-10-23").map(df.parse),
            Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")
          )
        }
      } finally {
        runner.shutdown()
      }
    }

    "set ingest counts" in {
      val catalog = nextCatalog()
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configureAccumuloService(runner, catalog)
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")

        var i = 0
        while (i < 3) {
          runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
          i += 1
        }
        runner.run(3)
        runner.assertTransferCount(Relationships.SuccessRelationship, i)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
        while (i > 0) {
          i -= 1
          val output = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship).get(i)
          output.assertAttributeEquals(org.geomesa.nifi.datastore.processor.Attributes.IngestSuccessCount, "3")
          output.assertAttributeEquals(org.geomesa.nifi.datastore.processor.Attributes.IngestFailureCount, "0")
        }
      } finally {
        runner.shutdown()
      }

      val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.CatalogParam.key -> catalog)).asJava)
      ds must not(beNull)
      try {
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
      } finally {
        ds.dispose()
      }
    }
  }
}
