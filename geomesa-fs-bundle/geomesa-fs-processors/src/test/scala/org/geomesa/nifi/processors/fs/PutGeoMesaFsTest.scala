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
import org.geotools.api.data.DataStoreFinder
import org.junit.{Assert, Test}
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

import java.nio.file.{Files, Path}
import java.util.Collections

class PutGeoMesaFsTest extends LazyLogging {

  @Test
  def testIngest(): Unit = {
    withDir { dir =>
      val path = dir.toFile.getAbsolutePath
      val runner = TestRunners.newTestRunner(new PutGeoMesa())
      try {
        val service = new FileSystemDataStoreService()
        runner.addControllerService("fs-datastore", service)
        runner.setProperty(service, FileSystemDataStoreParams.PathParam.key, path)
        runner.enableControllerService(service)

        runner.setProperty(DataStoreProcessor.Properties.DataStoreService, "fs-datastore")
        runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
        runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
        runner.setProperty(UserDataProcessor.Properties.SftUserData, "geomesa.fs.encoding=parquet\ngeomesa.fs.scheme={name=\"daily,z2-2bit\",options={}}")
        runner.enqueue(getClass.getClassLoader.getResourceAsStream("example.csv"))
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)
      } finally {
        runner.shutdown()
      }
      WithClose(DataStoreFinder.getDataStore(Collections.singletonMap(FileSystemDataStoreParams.PathParam.key, path))) { ds =>
        Assert.assertNotNull(ds)
        val sft = ds.getSchema("example")
        Assert.assertNotNull(sft)
        val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
        logger.debug(features.mkString(";"))
        Assert.assertEquals(3, features.length)
      }
    }
  }

  def withDir[T](fn: Path => T): T = {
    val dir = Files.createTempDirectory("gm-nifi-fs")
    try { fn(dir) } finally {
      PathUtils.deleteRecursively(dir)
    }
  }
}
