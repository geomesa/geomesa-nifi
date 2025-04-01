/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import org.apache.nifi.csv.{CSVRecordSetWriter, CSVUtils}
import org.apache.nifi.serialization.DateTimeUtils
import org.apache.nifi.util.TestRunners
import org.geomesa.nifi.datastore.processor.Relationships
import org.geomesa.nifi.datastore.processor.records.{GeoAvroRecordSetWriterFactory, Properties}
import org.geotools.api.data.{DataStoreFinder, Transaction}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.io.AvroDataFileReader
import org.locationtech.geomesa.kafka.KafkaContainerTest
import org.locationtech.geomesa.security.SecureSimpleFeature
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.runner.JUnitRunner

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

@RunWith(classOf[JUnitRunner])
class GetGeoMesaKafkaRecordTest extends KafkaContainerTest {

  sequential

  import scala.collection.JavaConverters._

  lazy val dsParams = Map(
    "kafka.brokers"    -> brokers,
    "kafka.zookeepers" -> zookeepers
  )

  val sftSpec: String = "string:String,int:Integer,double:Double,long:Long,float:Float," +
    "boolean:Boolean,uuid:UUID,pt:Point,date:Date,list:List[Int],map:Map[String,Int],bytes:Bytes"
  val sft: SimpleFeatureType = SimpleFeatureTypes.createType("example", sftSpec)

  val features: Seq[ScalaSimpleFeature] = Seq.tabulate(5) { i =>
    val sf = ScalaSimpleFeature.create(
      sft,
      s"$i", // fid
      s"string$i", // string
      s"$i", // int
      s"2.$i", // double
      s"$i", // long
      s"2.$i", // float
      i % 2 == 0, // bool
      s"${i}d2e799c-0652-4777-80c6-e8d8dbbb348e", // uuid
      s"POINT ($i 10)", // point
      s"2020-02-02T0$i:00:00.000Z", // date
      List(1, 2, i).asJava, // list
      Map(s"$i" -> i, s"2$i" -> (20 + i)).asJava, // map
      s"$i$i".getBytes(StandardCharsets.UTF_8) // byte array - csv outputs it with `new String(value)`
    )
    sf.getUserData.put("foo", Int.box(i % 3))
    sf.getUserData.put("bar", sf.getAttribute("date"))
    sf
  }

  val sftWithVis = SimpleFeatureTypes.createType("get-records-vis", "name:String,dtg:Date,*geom:Point:srid=4326")

  val featuresWithVis: Seq[ScalaSimpleFeature] = Seq.tabulate(5) { i =>
    val sf = ScalaSimpleFeature.create(sftWithVis, s"$i", s"name$i", s"2020-02-02T0$i:00:00.000Z", s"POINT ($i 10)")
    sf.visibility = i % 3 match {
      case 0 => "user"
      case 1 => "admin"
      case 2 => "user&admin"
    }
    sf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
      ds must not(beNull)
      ds.createSchema(sft)
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
      ds.createSchema(sftWithVis)
      WithClose(ds.getFeatureWriterAppend(sftWithVis.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
        featuresWithVis.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
    }
  }

  "GetGeoMesaKafkaRecord" should {
    "get records in csv format" in {
      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result = try {
        val service = new CSVRecordSetWriter()
        runner.addControllerService("csv-record-set-writer", service)
        runner.setProperty(service, DateTimeUtils.DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ssX")
        runner.enableControllerService(service)
        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "csv-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, java.util.UUID.randomUUID().toString)
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sft.getTypeName)
        runner.setProperty(GetGeoMesaKafkaRecord.ReplaceFeatureIds, "false")
        runner.setProperty(GetGeoMesaKafkaRecord.IncludeVisibilities, "false")
        dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.run()
        val results = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship)
        results.size mustEqual 1
        new String(runner.getContentAsByteArray(results.get(0)))
      } finally {
        runner.shutdown()
      }

      val lines = result.split("\n")
      lines must haveLength(6)
      lines.head mustEqual "id,string,int,double,long,float,boolean,uuid,pt,date,list,map,bytes"
      foreach(Range(0, 5)) { i =>
        // split up checks to avoid inconsistent ordering in the map iteration
        lines(i + 1) must startWith(s"""$i,string$i,$i,2.$i,$i,2.$i,${i % 2 == 0},${i}d2e799c-0652-4777-80c6-e8d8dbbb348e,POINT ($i 10),2020-02-02T0$i:00:00Z,"[1, 2, $i]","{""")
        foreach(Seq(s"$i=$i", s"2$i=2$i"))(entry => lines(i + 1) must contain(entry))
        lines(i + 1) must endWith(s"""}",$i$i""")
      }
    }

    "get records with visibilities" in {
      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result = try {
        val service = new CSVRecordSetWriter()
        runner.addControllerService("csv-record-set-writer", service)
        runner.setProperty(service, DateTimeUtils.DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ssX")
        runner.enableControllerService(service)
        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "csv-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, java.util.UUID.randomUUID().toString)
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sftWithVis.getTypeName)
        runner.setProperty(GetGeoMesaKafkaRecord.ReplaceFeatureIds, "false")
        runner.setProperty(GetGeoMesaKafkaRecord.IncludeVisibilities, "true")
        dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.run()
        val results = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship)
        results.size mustEqual 1
        new String(runner.getContentAsByteArray(results.get(0)))
      } finally {
        runner.shutdown()
      }

      result mustEqual
          """id,name,dtg,geom,visibilities
            |0,name0,2020-02-02T00:00:00Z,POINT (0 10),user
            |1,name1,2020-02-02T01:00:00Z,POINT (1 10),admin
            |2,name2,2020-02-02T02:00:00Z,POINT (2 10),user&admin
            |3,name3,2020-02-02T03:00:00Z,POINT (3 10),user
            |4,name4,2020-02-02T04:00:00Z,POINT (4 10),admin
            |""".stripMargin
    }

    "get records with user data" in {
      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result = try {
        val service = new CSVRecordSetWriter()
        runner.addControllerService("csv-record-set-writer", service)
        runner.setProperty(service, DateTimeUtils.DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ssX")
        runner.setProperty(service, CSVUtils.QUOTE_CHAR, "'")
        runner.enableControllerService(service)
        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "csv-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, java.util.UUID.randomUUID().toString)
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sft.getTypeName)
        runner.setProperty(GetGeoMesaKafkaRecord.IncludeUserData, "true")
        runner.setProperty(GetGeoMesaKafkaRecord.ReplaceFeatureIds, "false")
        runner.setProperty(GetGeoMesaKafkaRecord.IncludeVisibilities, "false")
        dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.run()
        val results = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship)
        results.size mustEqual 1
        new String(runner.getContentAsByteArray(results.get(0)))
      } finally {
        runner.shutdown()
      }

      val lines = result.split("\n")
      lines must haveLength(6)
      lines.head mustEqual "id,string,int,double,long,float,boolean,uuid,pt,date,list,map,bytes,user-data"
      foreach(Range(0, 5)) { i =>
        // split up checks to avoid inconsistent ordering in the map iteration
        lines(i + 1) must startWith(s"""$i,string$i,$i,2.$i,$i,2.$i,${i % 2 == 0},${i}d2e799c-0652-4777-80c6-e8d8dbbb348e,POINT ($i 10),2020-02-02T0$i:00:00Z,'[1, 2, $i]','{""")
        foreach(Seq(s"$i=$i", s"2$i=2$i"))(entry => lines(i + 1) must contain(entry))
        lines(i + 1) must endWith(s"""}',$i$i,'{"bar":"2020-02-02T0$i:00:00.000Z","foo":${i % 3}}'""")
      }
    }

    "get records in GeoAvro format" in {
      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result: Array[Byte] = try {
        val service = new GeoAvroRecordSetWriterFactory()
        runner.addControllerService("avro-record-set-writer", service)
        runner.enableControllerService(service)

        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "avro-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, java.util.UUID.randomUUID().toString)
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sft.getTypeName)
        runner.setProperty(GetGeoMesaKafkaRecord.ReplaceFeatureIds, "false")
        runner.setProperty(GetGeoMesaKafkaRecord.IncludeVisibilities, "false")
        dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.run()
        val results = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship)
        results.size mustEqual 1
        runner.getContentAsByteArray(results.get(0))
      } finally {
        runner.shutdown()
      }

      val featuresRead = WithClose(new AvroDataFileReader(new ByteArrayInputStream(result)))(_.toList)
      featuresRead must haveSize(5)
    }

    "get records in GeoAvro format with visibilities" in {
      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result: Array[Byte] = try {
        val service = new GeoAvroRecordSetWriterFactory()
        runner.addControllerService("avro-record-set-writer", service)
        runner.setProperty(service, Properties.VisibilitiesCol, "visibilities")
        runner.enableControllerService(service)

        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "avro-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, java.util.UUID.randomUUID().toString)
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sftWithVis.getTypeName)
        runner.setProperty(GetGeoMesaKafkaRecord.ReplaceFeatureIds, "false")
        runner.setProperty(GetGeoMesaKafkaRecord.IncludeVisibilities, "true")
        dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.run()
        val results = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship)
        results.size mustEqual 1
        runner.getContentAsByteArray(results.get(0))
      } finally {
        runner.shutdown()
      }

      val featuresRead = WithClose(new AvroDataFileReader(new ByteArrayInputStream(result)))(_.toList)
      featuresRead must haveSize(5)
      featuresRead.head.visibility must beSome
    }

    "get records in GeoAvro format suitable for ingest into GeoMesa with minimal configuration" in {
      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result: Array[Byte] = try {
        val service = new GeoAvroRecordSetWriterFactory()
        runner.addControllerService("avro-record-set-writer", service)
        runner.enableControllerService(service)

        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "avro-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, java.util.UUID.randomUUID().toString)
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sftWithVis.getTypeName)
        dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.run()
        val results = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship)
        results.size mustEqual 1
        runner.getContentAsByteArray(results.get(0))
      } finally {
        runner.shutdown()
      }

      val featuresRead = WithClose(new AvroDataFileReader(new ByteArrayInputStream(result))) { reader =>
        reader.toList.sortBy(_.getAttribute("int").asInstanceOf[Int])
      }
      featuresRead must haveSize(5)
      featuresRead.flatMap(_.visibility) mustEqual Seq("user", "admin", "user&admin", "user", "admin")
      // verify feature id was replaced
      featuresRead.map(_.getID) must not(containAnyOf(featuresWithVis.map(_.getID)))
      featuresRead.map(_.getAttributes) mustEqual featuresWithVis.map(_.getAttributes)
    }
  }
}
