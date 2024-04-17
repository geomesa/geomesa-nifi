/***********************************************************************
 * Copyright (c) 2015-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.util.TestRunners
import org.geomesa.nifi.datastore.processor.mixins.{ConvertInputProcessor, FeatureTypeProcessor}
import org.junit.{Assert, Test}

import java.nio.file.Paths

class ConvertToGeoFileTest extends LazyLogging {

  @Test
  def testConvert(): Unit = {
    val runner = TestRunners.newTestRunner(new ConvertToGeoFile())
    try {
      val input = Paths.get(getClass.getClassLoader.getResource("example.csv").toURI)
      runner.setProperty(ConvertToGeoFile.Properties.IncludeHeaders, "false")
      runner.setProperty(FeatureTypeProcessor.Properties.SftNameKey, "example")
      runner.setProperty(ConvertInputProcessor.Properties.ConverterNameKey, "example-csv")
      ConvertToGeoFile.Formats.zipWithIndex.foreach { case (format, i) =>
        runner.setProperty(ConvertToGeoFile.Properties.OutputFormat, format)
        runner.enqueue(input)
        runner.run()
        runner.assertTransferCount(Relationships.SuccessRelationship, i + 1)
        runner.assertTransferCount(Relationships.OriginalRelationship, i + 1)
        runner.assertTransferCount(Relationships.FailureRelationship, 0)

        val original = runner.getFlowFilesForRelationship(Relationships.OriginalRelationship).get(i)
        original.assertAttributeEquals("geomesa.convert.successes", "3")
        original.assertAttributeEquals("geomesa.convert.failures", "0")
        original.assertContentEquals(input)

        val output = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship).get(i)
        output.assertAttributeEquals("geomesa.convert.successes", "3")
        output.assertAttributeEquals("geomesa.convert.failures", "0")

        val ext = format match {
          case "arrow"        => Assert.assertTrue(output.getData.length > 0); format
          case "avro"         => Assert.assertTrue(output.getData.length > 0); format
          case "avro-native"  => Assert.assertTrue(output.getData.length > 0); "avro"
          case "bin"          => Assert.assertEquals(48, output.getData.length); format
          case "csv"          => output.assertContentEquals(csv); format
          case "gml2"         => output.assertContentEquals(gml2); "xml"
          case "gml"          => output.assertContentEquals(gml3); "xml"
          case "json"         => output.assertContentEquals(json); format
          case "leaflet"      => Assert.assertTrue(output.getData.length > 0); "html"
          case "orc"          => Assert.assertTrue(output.getData.length > 0); format
          case "parquet"      => Assert.assertTrue(output.getData.length > 0); format
          case "tsv"          => output.assertContentEquals(tsv); format
          case _              => Assert.fail(s"No case for output format $format")
        }
        output.assertAttributeEquals("filename", s"example.$ext")
      }
    } finally {
      runner.shutdown()
    }
  }

  lazy val csv =
    "23623,Harry,20,2015-05-06T00:00:00.000Z,POINT (-100.2365 23)\r\n" +
        "26236,Hermione,25,2015-06-07T00:00:00.000Z,POINT (40.232 -53.2356)\r\n" +
        "3233,Severus,30,2015-10-23T00:00:00.000Z,POINT (3 -62.23)\r\n"

  lazy val tsv =
    "23623\tHarry\t20\t2015-05-06T00:00:00.000Z\tPOINT (-100.2365 23)\r\n" +
        "26236\tHermione\t25\t2015-06-07T00:00:00.000Z\tPOINT (40.232 -53.2356)\r\n" +
        "3233\tSeverus\t30\t2015-10-23T00:00:00.000Z\tPOINT (3 -62.23)\r\n"

  lazy val json =
    """{"type":"FeatureCollection","features":[""" +
      """{"type":"Feature","id":"23623","geometry":{"type":"Point","coordinates":[-100.2365,23]},"properties":{"name":"Harry","age":20,"dtg":"2015-05-06T00:00:00.000Z"}},""" +
      """{"type":"Feature","id":"26236","geometry":{"type":"Point","coordinates":[40.232,-53.2356]},"properties":{"name":"Hermione","age":25,"dtg":"2015-06-07T00:00:00.000Z"}},""" +
      """{"type":"Feature","id":"3233","geometry":{"type":"Point","coordinates":[3,-62.23]},"properties":{"name":"Severus","age":30,"dtg":"2015-10-23T00:00:00.000Z"}}]}""" +
      "\n"

  lazy val gml2 =
    """<?xml version="1.0" encoding="UTF-8"?><wfs:FeatureCollection xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:geomesa="http://geomesa.org" xmlns:wfs="http://www.opengis.net/wfs" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc">
      |  <gml:boundedBy>
      |    <gml:Box srsName="http://www.opengis.net/gml/srs/epsg.xml#4326">
      |      <gml:coord>
      |        <gml:X>-180.0</gml:X>
      |        <gml:Y>-90.0</gml:Y>
      |      </gml:coord>
      |      <gml:coord>
      |        <gml:X>180.0</gml:X>
      |        <gml:Y>90.0</gml:Y>
      |      </gml:coord>
      |    </gml:Box>
      |  </gml:boundedBy>
      |  <gml:featureMember>
      |    <geomesa:example fid="23623">
      |      <gml:name>Harry</gml:name>
      |      <geomesa:age>20</geomesa:age>
      |      <geomesa:dtg>2015-05-06T00:00:00.000Z</geomesa:dtg>
      |      <geomesa:geom>
      |        <gml:Point srsName="http://www.opengis.net/gml/srs/epsg.xml#4326">
      |          <gml:coordinates>-100.2365,23</gml:coordinates>
      |        </gml:Point>
      |      </geomesa:geom>
      |    </geomesa:example>
      |  </gml:featureMember>
      |  <gml:featureMember>
      |    <geomesa:example fid="26236">
      |      <gml:name>Hermione</gml:name>
      |      <geomesa:age>25</geomesa:age>
      |      <geomesa:dtg>2015-06-07T00:00:00.000Z</geomesa:dtg>
      |      <geomesa:geom>
      |        <gml:Point srsName="http://www.opengis.net/gml/srs/epsg.xml#4326">
      |          <gml:coordinates>40.232,-53.2356</gml:coordinates>
      |        </gml:Point>
      |      </geomesa:geom>
      |    </geomesa:example>
      |  </gml:featureMember>
      |  <gml:featureMember>
      |    <geomesa:example fid="3233">
      |      <gml:name>Severus</gml:name>
      |      <geomesa:age>30</geomesa:age>
      |      <geomesa:dtg>2015-10-23T00:00:00.000Z</geomesa:dtg>
      |      <geomesa:geom>
      |        <gml:Point srsName="http://www.opengis.net/gml/srs/epsg.xml#4326">
      |          <gml:coordinates>3,-62.23</gml:coordinates>
      |        </gml:Point>
      |      </geomesa:geom>
      |    </geomesa:example>
      |  </gml:featureMember>
      |</wfs:FeatureCollection>
      |""".stripMargin

  lazy val gml3 =
    """<?xml version="1.0" encoding="UTF-8"?><wfs:FeatureCollection xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:geomesa="http://geomesa.org" xmlns:wfs="http://www.opengis.net/wfs" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink">
      |  <gml:boundedBy>
      |    <gml:Envelope srsDimension="2" srsName="http://www.opengis.net/gml/srs/epsg.xml#4326">
      |      <gml:lowerCorner>-180 -90</gml:lowerCorner>
      |      <gml:upperCorner>180 90</gml:upperCorner>
      |    </gml:Envelope>
      |  </gml:boundedBy>
      |  <gml:featureMembers>
      |    <geomesa:example gml:id="23623">
      |      <gml:name>Harry</gml:name>
      |      <geomesa:age>20</geomesa:age>
      |      <geomesa:dtg>2015-05-06T00:00:00.000Z</geomesa:dtg>
      |      <geomesa:geom>
      |        <gml:Point srsName="http://www.opengis.net/gml/srs/epsg.xml#4326" srsDimension="2">
      |          <gml:pos>-100.2365 23</gml:pos>
      |        </gml:Point>
      |      </geomesa:geom>
      |    </geomesa:example>
      |    <geomesa:example gml:id="26236">
      |      <gml:name>Hermione</gml:name>
      |      <geomesa:age>25</geomesa:age>
      |      <geomesa:dtg>2015-06-07T00:00:00.000Z</geomesa:dtg>
      |      <geomesa:geom>
      |        <gml:Point srsName="http://www.opengis.net/gml/srs/epsg.xml#4326" srsDimension="2">
      |          <gml:pos>40.232 -53.2356</gml:pos>
      |        </gml:Point>
      |      </geomesa:geom>
      |    </geomesa:example>
      |    <geomesa:example gml:id="3233">
      |      <gml:name>Severus</gml:name>
      |      <geomesa:age>30</geomesa:age>
      |      <geomesa:dtg>2015-10-23T00:00:00.000Z</geomesa:dtg>
      |      <geomesa:geom>
      |        <gml:Point srsName="http://www.opengis.net/gml/srs/epsg.xml#4326" srsDimension="2">
      |          <gml:pos>3 -62.23</gml:pos>
      |        </gml:Point>
      |      </geomesa:geom>
      |    </geomesa:example>
      |  </gml:featureMembers>
      |</wfs:FeatureCollection>
      |""".stripMargin

}
