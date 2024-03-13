/*
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.nifi.accumulo.nar

import org.geomesa.nifi.datastore.processor.NiFiContainer
import org.geomesa.nifi.processors.accumulo.AccumuloDataStoreService
import org.geomesa.testcontainers.AccumuloContainer
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.testcontainers.containers.{BindMode, Network}
import org.testcontainers.utility.DockerImageName

import java.io.InputStream
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class AccumuloNarIT extends Specification {

  import scala.collection.JavaConverters._

  private val network = Network.newNetwork()
  private val catalog = getClass.getSimpleName

  private var accumuloContainer: AccumuloContainer = _
  private var nifiContainer: NiFiContainer = _

  lazy private val accumuloName =
    DockerImageName.parse("ghcr.io/geomesa/accumulo-uno")
        .withTag(sys.props.getOrElse("accumulo.it.version", "2.1.2"))

  lazy private val params = Map(
    AccumuloDataStoreParams.UserParam.key         -> accumuloContainer.getUsername,
    AccumuloDataStoreParams.PasswordParam.key     -> accumuloContainer.getPassword,
    AccumuloDataStoreParams.InstanceNameParam.key -> accumuloContainer.getInstanceName,
    AccumuloDataStoreParams.ZookeepersParam.key   -> accumuloContainer.getZookeepers,
    AccumuloDataStoreParams.CatalogParam.key      -> catalog
  )

  step {
    accumuloContainer =
      new AccumuloContainer(accumuloName)
          .withGeoMesaDistributedRuntime()
          .withNetwork(network)
    accumuloContainer.start()

    // instead of setting the connection props in the processor (which requires encrypting them),
    // mount the accumulo-client.properties where the processor can pick it up
    val accumuloClientProps =
      accumuloContainer.copyFileFromContainer(
        "/opt/fluo-uno/install/accumulo/conf/accumulo-client.properties",
        (is: InputStream) => NiFiContainer.writeTempFile("accumulo-client.properties", is).toFile.getAbsolutePath)
    val clientPropsMountPath = "/opt/nifi/nifi-current/conf/accumulo-client.properties"

    nifiContainer =
      new NiFiContainer()
          .withDefaultIngestFlow("accumulo21", classOf[AccumuloDataStoreService], Map("accumulo.catalog" -> catalog))
          .withFileSystemBind(accumuloClientProps, clientPropsMountPath, BindMode.READ_ONLY)
          .withNetwork(network)
    nifiContainer.start()
  }

  "Accumulo 2.1 NAR" should {
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
  }

  step {
    if (nifiContainer != null) {
      nifiContainer.stop()
    }
    if (accumuloContainer != null) {
      accumuloContainer.stop()
    }
  }
}
