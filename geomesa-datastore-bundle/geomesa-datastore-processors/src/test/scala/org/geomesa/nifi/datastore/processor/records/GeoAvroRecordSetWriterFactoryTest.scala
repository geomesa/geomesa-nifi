/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.records

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.csv.{CSVReader, CSVUtils}
import org.apache.nifi.processors.standard.ConvertRecord
import org.apache.nifi.util.{TestRunner, TestRunners}
import org.geomesa.nifi.datastore.processor.records.Properties.{GeometryCols, TypeName}
import org.geotools.api.feature.simple.SimpleFeature
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.io.AvroDataFileReader
import org.locationtech.jts.geom.{Geometry, MultiPolygon}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.ByteArrayInputStream

@RunWith(classOf[JUnitRunner])
class GeoAvroRecordSetWriterFactoryTest extends Specification with LazyLogging {
  sequential

  "GeoAvroRecordSetWriterFactory" should {
    "Write out GeoAvro from a simple Record producing Processor" in {
      val content: String =
        "id|username|role|position\n" +
          "123|Legend|actor|POINT(-118.3287 34.0928)\n" +
          "456|Lewis|leader|POINT(-86.9023 4.567)\n" +
          "789|Basie|pianist|POINT(-73.9465 40.8116)\n"

      val geometryColumns = "position:Point"

      val featuresRead: Seq[SimpleFeature] = configureAndRun(content, geometryColumns, "test")

      featuresRead.size mustEqual 3
    }

    "Write out all geometry types and handle default geometries" in {
      val geometries = s"MULTILINESTRING((0 0, 10 10))|" +
      "POINT(0 0)|" + "LINESTRING(0 0, 1 1, 4 4)|" +
      "POLYGON((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11))|" +
      "MULTIPOINT((0 0), (1 1))|" +
      "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))|" +
      "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11)))"

      val content: String =
        "id|username|role|position|defaultmls|p|ls|poly|mp|mls|mpoly\n" +
          s"123|Legend|actor|POINT(-118.3287 34.0928)|$geometries\n" +
          s"456|Lewis|leader|POINT(-86.9023 4.567)|$geometries\n" +
          s"789|Basie|pianist|POINT(-73.9465 40.8116)|$geometries\n"

      val geometryColumns = "*defaultmls:MultiLineString,position:Point,p:Point,ls:LineString,poly:Polygon,mp:MultiPoint,mls:MultiLineString,mpoly:MultiPolygon"
      val typeName = "testTypeName"

      val featuresRead: Seq[SimpleFeature] = configureAndRun(content, geometryColumns, typeName)
      featuresRead.size mustEqual 3

      val feature = featuresRead.head
      // Check the various geometry types were instantiated as a geometry rather than just a String
      (3 until 11).foreach { i =>
        feature.getAttribute(i) must anInstanceOf[Geometry]
      }

      val sft = feature.getFeatureType
      sft.getGeometryDescriptor.getName.getLocalPart mustEqual "defaultmls"

      sft.getTypeName mustEqual "testTypeName"

      // Spot check for a given type/field
      sft.getDescriptor("mpoly").getType.getBinding mustEqual classOf[MultiPolygon]
    }
  }

  private def configureAndRun(content: String, geometryColumns: String, typeName: String) = {
    val runner: TestRunner = buildRunner(geometryColumns, typeName)
    enqueueAndRun(runner, content)

    val featuresRead: Seq[SimpleFeature] = getFeatures(runner)
    featuresRead
  }

  private def enqueueAndRun(runner: TestRunner, content: String): Unit = {
    runner.enqueue(content)
    runner.run()

    runner.assertAllFlowFilesTransferred("success", 1)
  }

  private def getFeatures(runner: TestRunner) = {
    val result = runner.getContentAsByteArray(runner.getFlowFilesForRelationship("success").get(0))

    val bais = new ByteArrayInputStream(result)
    val avroReader = new AvroDataFileReader(bais)
    val featuresRead: Seq[SimpleFeature] = avroReader.toList
    featuresRead
  }

  private def buildRunner(geometryColumns: String, typeName: String) = {
    val runner: TestRunner = TestRunners.newTestRunner(classOf[ConvertRecord])

    val csvReader: CSVReader = new CSVReader
    runner.addControllerService("csv-reader", csvReader)
    runner.setProperty(csvReader, CSVUtils.VALUE_SEPARATOR, "|")
    runner.enableControllerService(csvReader)

    val geoAvroWriter = new GeoAvroRecordSetWriterFactory()
    runner.addControllerService("geo-avro-record-set-writer", geoAvroWriter)
    runner.setProperty(geoAvroWriter, GeometryCols, geometryColumns)
    runner.setProperty(geoAvroWriter, TypeName, typeName)
    runner.enableControllerService(geoAvroWriter)

    runner.setProperty("Record Reader", "csv-reader")
    runner.setProperty("Record Writer", "geo-avro-record-set-writer")
    runner
  }
}
