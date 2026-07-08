/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.fs

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.util.TestRunners
import org.geomesa.nifi.datastore.processor.mixins.{ConvertInputProcessor, DataStoreProcessor, FeatureTypeProcessor, UserDataProcessor}
import org.geomesa.nifi.datastore.processor.{PutGeoMesa, Relationships}
import org.geomesa.nifi.processors.fs.PutGeoMesaFsTest.IcebergRestContainer
import org.geotools.api.data.DataStoreFinder
import org.locationtech.geomesa.fs.data.FileSystemDataStoreParams
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.{GenericContainer, MinIOContainer, Network}
import org.testcontainers.utility.DockerImageName

class PutGeoMesaFsTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  import scala.collection.JavaConverters._

  private val network = Network.newNetwork()

  private val minio =
    new MinIOContainer(DockerImageName.parse("minio/minio").withTag(sys.props("minio.docker.tag")))
      .withNetwork(network)
      .withNetworkAliases("minio")

  private val iceberg =
    new IcebergRestContainer()
      .withNetwork(network)
      .withNetworkAliases("rest-catalog")
      .withEnv("CATALOG_WAREHOUSE", "s3://geomesa/iceberg")
      .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
      .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
      .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
      .withEnv("AWS_REGION", "us-east-1")
      .withEnv("AWS_ACCESS_KEY_ID", "minioadmin")
      .withEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")

  override def beforeAll(): Unit = {
    minio.start()
    minio.execInContainer("mc", "alias", "set", "localhost", "http://localhost:9000", minio.getUserName, minio.getPassword)
    minio.execInContainer("mc", "mb", "localhost/geomesa")
    iceberg.start()
  }

  override def afterAll(): Unit = {
    iceberg.stop()
    minio.stop()
  }

  "PutGeoMesaFs" should {
    "ingest" in {
      val params =
        Map(
          FileSystemDataStoreParams.PathParam.key -> "s3://geomesa/fs/",
          FileSystemDataStoreParams.ConfigParam.key ->
            s"""type=rest
               |uri=http://${iceberg.getHost}:${iceberg.getFirstMappedPort}/
               |iceberg.namespace=geomesa
               |fs.s3.region=us-east-1
               |fs.s3.endpoint=${minio.getS3URL}
               |fs.s3.access-key-id=${minio.getUserName}
               |fs.s3.secret-access-key=${minio.getPassword}
               |fs.s3.force-path-style=true
               |""".stripMargin,
        )
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        val service = new FileSystemDataStoreService()
        runner.addControllerService("fs-datastore", service)
        params.foreach { case (k, v) =>
          runner.setProperty(service, k, v)
        }
        runner.enableControllerService(service)

        runner.setProperty(DataStoreProcessor.Properties.DataStoreService, "fs-datastore")
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
        runner.setProperty(UserDataProcessor.Properties.SftUserData, "geomesa.fs.scheme=daily,z2:bits=4")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }
      WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        ds must not(beNull)
        val sft = ds.getSchema("example")
        sft must not(beNull)
        val features = CloseableIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        features must haveLength(3)
      }
    }
  }
}

object PutGeoMesaFsTest {

  val IcebergRestImage = DockerImageName.parse("apache/iceberg-rest-fixture").withTag(sys.props("iceberg.rest.docker.tag"))

  class IcebergRestContainer extends GenericContainer[IcebergRestContainer](IcebergRestImage) {
    withExposedPorts(8181)
  }
}
