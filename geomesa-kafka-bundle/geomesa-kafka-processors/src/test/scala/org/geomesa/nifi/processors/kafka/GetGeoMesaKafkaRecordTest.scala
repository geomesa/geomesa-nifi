/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.csv.CSVRecordSetWriter
import org.apache.nifi.serialization.DateTimeUtils
import org.apache.nifi.util.TestRunners
import org.geomesa.nifi.datastore.processor.Relationships
import org.geomesa.nifi.datastore.processor.records.GeoAvroRecordSetWriterFactory
import org.geotools.data.{DataStoreFinder, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.kafka.EmbeddedKafka
import org.locationtech.geomesa.security.SecureSimpleFeature
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GetGeoMesaKafkaRecordTest extends Specification with LazyLogging {
  sequential

  import scala.collection.JavaConverters._

  var kafka: EmbeddedKafka = _

  step {
    kafka = new EmbeddedKafka()
  }

  lazy val dsParams = Map(
    "kafka.brokers"    -> kafka.brokers,
    "kafka.zookeepers" -> kafka.zookeepers
  )

  val sftSpec: String = "string:String,int:Integer,double:Double,long:Long,float:Float," +
    "boolean:Boolean,uuid:UUID,pt:Point,date:Date,list:List[Int],map:Map[String,Int],bytes:Bytes"
  val sft: SimpleFeatureType = SimpleFeatureTypes.createType("example", sftSpec)

  val features: Seq[ScalaSimpleFeature] = Seq.tabulate(5) { i =>
    ScalaSimpleFeature.create(
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

  "GetGeoMesaKafkaRecord" should {
    "get records in csv format" in {
      val sft =
        SimpleFeatureTypes.createType("get-records", sftSpec)

      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
        ds must not(beNull)
        ds.createSchema(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }

      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result = try {
        val service = new CSVRecordSetWriter()
        runner.addControllerService("csv-record-set-writer", service)
        runner.setProperty(service, DateTimeUtils.DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ssX")
        runner.enableControllerService(service)
        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "csv-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, "test-id")
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sft.getTypeName)
        dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.run()
        val results = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship)
        results.size mustEqual 1
        new String(runner.getContentAsByteArray(results.get(0)))
      } finally {
        runner.shutdown()
      }

      result mustEqual
          """id,string,int,double,long,float,boolean,uuid,pt,date,list,map,bytes
            |0,string0,0,2.0,0,2.0,true,0d2e799c-0652-4777-80c6-e8d8dbbb348e,POINT (0 10),2020-02-02T00:00:00Z,"[1, 2, 0]","{20=20, 0=0}",00
            |1,string1,1,2.1,1,2.1,false,1d2e799c-0652-4777-80c6-e8d8dbbb348e,POINT (1 10),2020-02-02T01:00:00Z,"[1, 2, 1]","{21=21, 1=1}",11
            |2,string2,2,2.2,2,2.2,true,2d2e799c-0652-4777-80c6-e8d8dbbb348e,POINT (2 10),2020-02-02T02:00:00Z,"[1, 2, 2]","{2=2, 22=22}",22
            |3,string3,3,2.3,3,2.3,false,3d2e799c-0652-4777-80c6-e8d8dbbb348e,POINT (3 10),2020-02-02T03:00:00Z,"[1, 2, 3]","{23=23, 3=3}",33
            |4,string4,4,2.4,4,2.4,true,4d2e799c-0652-4777-80c6-e8d8dbbb348e,POINT (4 10),2020-02-02T04:00:00Z,"[1, 2, 4]","{24=24, 4=4}",44
            |""".stripMargin
    }

    "get records with visibilities" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
        ds must not(beNull)
        ds.createSchema(sftWithVis)
        WithClose(ds.getFeatureWriterAppend(sftWithVis.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          featuresWithVis.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }

      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result = try {
        val service = new CSVRecordSetWriter()
        runner.addControllerService("csv-record-set-writer", service)
        runner.setProperty(service, DateTimeUtils.DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ssX")
        runner.enableControllerService(service)
        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "csv-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, "test-id")
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sftWithVis.getTypeName)
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

    "get records in GeoAvro format" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
        ds must not(beNull)
        ds.createSchema(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }

      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result: Array[Byte] = try {
        val service = new GeoAvroRecordSetWriterFactory()
        runner.addControllerService("avro-record-set-writer", service)
        runner.enableControllerService(service)

        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "avro-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, "test-id")
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sft.getTypeName)
        dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.run()
        val results = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship)
        results.size mustEqual 1
        runner.getContentAsByteArray(results.get(0))
      } catch {
        case t: Throwable =>
          println(s"Caught $t")
          throw t
      } finally {
        runner.shutdown()
      }

      val bais = new ByteArrayInputStream(result)
      val avroReader = new AvroDataFileReader(bais)
      val featuresRead: Seq[SimpleFeature] = avroReader.toList

      featuresRead.foreach { println }
      featuresRead.size mustEqual 5
    }

    "get records in GeoAvro format with visibilities" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
        ds must not(beNull)
        ds.createSchema(sftWithVis)
        WithClose(ds.getFeatureWriterAppend(sftWithVis.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          featuresWithVis.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }

      val runner = TestRunners.newTestRunner(new GetGeoMesaKafkaRecord())
      val result: Array[Byte] = try {
        val service = new GeoAvroRecordSetWriterFactory()
        runner.addControllerService("avro-record-set-writer", service)
        runner.setProperty(service, GeoAvroRecordSetWriterFactory.VisibilitiesColumn, "visibilities")
        runner.enableControllerService(service)

        runner.setProperty(GetGeoMesaKafkaRecord.RecordWriter, "avro-record-set-writer")
        runner.setProperty(GetGeoMesaKafkaRecord.GroupId, "test-id")
        runner.setProperty(GetGeoMesaKafkaRecord.InitialOffset, "earliest")
        runner.setProperty(GetGeoMesaKafkaRecord.RecordMaxBatchSize, "5")
        runner.setProperty(GetGeoMesaKafkaRecord.TypeName, sftWithVis.getTypeName)
        runner.setProperty(GetGeoMesaKafkaRecord.IncludeVisibilities, "true")
        dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
        runner.run()
        val results = runner.getFlowFilesForRelationship(Relationships.SuccessRelationship)
        results.size mustEqual 1
        runner.getContentAsByteArray(results.get(0))
      } catch {
        case t: Throwable =>
          println(s"Caught $t")
          throw t
      } finally {
        runner.shutdown()
      }

      val bais = new ByteArrayInputStream(result)
      val avroReader = new AvroDataFileReader(bais)
      val featuresRead: Seq[SimpleFeature] = avroReader.toList

      featuresRead.foreach { println }
      featuresRead.head.visibility.get must not be null
      featuresRead.size mustEqual 5
    }
  }

  step {
    kafka.close()
  }
}
