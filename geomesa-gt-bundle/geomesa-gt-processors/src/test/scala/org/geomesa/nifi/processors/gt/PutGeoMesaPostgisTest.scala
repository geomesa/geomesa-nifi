/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.gt

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.util.{TestRunner, TestRunners}
import org.geomesa.nifi.datastore.processor._
import org.geomesa.nifi.datastore.processor.mixins.DataStoreProcessor
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.data.{DataStore, DataStoreFinder}
import org.junit.runner.RunWith
import org.locationtech.geomesa.gt.partition.postgis.PartitionedPostgisDataStoreParams
import org.locationtech.jts.geom.Point
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.images.builder.ImageFromDockerfile

import java.sql.Timestamp

@RunWith(classOf[JUnitRunner])
class PutGeoMesaPostgisTest extends Specification with BeforeAfterAll with LazyLogging {

  import scala.collection.JavaConverters._

  lazy val params = Map(
    "dbtype" -> PartitionedPostgisDataStoreParams.DbType.sample.toString,
    "host" -> host,
    "port" -> port,
    "schema" -> "public",
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> "postgres",
    "Batch insert size" -> "10",
    "preparedStatements" -> "true"
  )

  var container: GenericContainer[_] = _

  lazy val host = Option(container).map(_.getHost).getOrElse("localhost")
  lazy val port = Option(container).map(_.getFirstMappedPort).getOrElse(5432).toString

  override def beforeAll(): Unit = {
    val image =
      new ImageFromDockerfile("testcontainers/postgis_cron_nifi", false)
          .withFileFromClasspath(".", "testcontainers")
          .withBuildArg("FROM_TAG", sys.props.getOrElse("postgis.docker.tag", "15-3.3"))
    container = new GenericContainer(image)
    container.addEnv("POSTGRES_HOST_AUTH_METHOD", "trust")
    container.addExposedPort(5432)
    container.start()
    container.followOutput(new Slf4jLogConsumer(logger.underlying))
  }

  override def afterAll(): Unit = {
    if (container != null) {
      container.stop()
    }
  }

  def configurePostgisService(runner: TestRunner): Unit = {
    val service = new PartitionedPostgisDataStoreService()
    runner.addControllerService("data-store", service)
    params.foreach { case (k, v) => runner.setProperty(service, k, v) }
    runner.enableControllerService(service)
    runner.setProperty(DataStoreProcessor.Properties.DataStoreService, "data-store")
  }

  "PutGeoMesa" should {
    "initialize schemas on startup" in {
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configurePostgisService(runner)
        runner.setProperty(PutGeoMesa.Properties.InitSchemas,
          "test=test\nfoo=test\nbar=name:String,dtg:Date,*geom:Point:srid=4326")
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 0)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }
  
      val ds = DataStoreFinder.getDataStore(params.asJava)
      ds must not(beNull)
      try {
        foreach(Seq("test", "foo", "bar")) { typeName =>
          val sft = ds.getSchema(typeName)
          sft must not(beNull)
          sft.getAttributeCount mustEqual 3
          sft.getDescriptor(0).getLocalName mustEqual "name"
          sft.getDescriptor(0).getType.getBinding mustEqual classOf[String]
          sft.getDescriptor(1).getLocalName mustEqual "dtg"
          sft.getDescriptor(1).getType.getBinding mustEqual classOf[Timestamp]
          sft.getDescriptor(2).getLocalName mustEqual "geom"
          sft.getDescriptor(2).getType.getBinding mustEqual classOf[Point]
        }
      } finally {
        ds.dispose()
      }
    }
  
    "validate init schemas" in {
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configurePostgisService(runner)
        runner.setProperty(PutGeoMesa.Properties.InitSchemas, "test=foo\nfoo=bar")
        runner.assertNotValid()
        ok
      } finally {
        runner.shutdown()
      }
    }

    "Create separate data stores to prevent synchronization around writers" in {
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        configurePostgisService(runner)
        val service = runner.getControllerService[DataStoreService]("data-store", classOf[DataStoreService])
        val Array(store1, store2, store3) = service.loadDataStores(3).toArray(Array.empty[DataStore])
        try {
          store1 must not(be(store2))
          store2 must not(be(store3))
          store3 must not(be(store1))
        } finally {
          Seq(store1, store2, store3).foreach(_.dispose())
        }
      } finally {
        runner.shutdown()
      }
    }
  }
}
