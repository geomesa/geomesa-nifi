/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.records

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.`type`.RecordDataType
import org.apache.nifi.serialization.record.{MapRecord, RecordField, RecordFieldType, StandardSchemaIdentifier}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Point
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Date

@RunWith(classOf[JUnitRunner])
class SimpleFeatureRecordConverterTest extends Specification with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  "SimpleFeatureRecordConverter" should {

    "create a converter based on a feature type" in {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

      val converter = SimpleFeatureRecordConverter(sft)
      converter.schema.getSchemaName.orElse(null) mustEqual "test"
      converter.schema.getFieldNames.asScala mustEqual Seq("id", "name", "age", "dtg", "geom")

      val feature = ScalaSimpleFeature.create(sft, "id", "myname", "10", "2020-01-01T00:00:00.000Z", "POINT (45 55)")
      val record = converter.convert(feature)
      record.getValue("id") mustEqual "id"
      record.getValue("name") mustEqual "myname"
      record.getValue("age") mustEqual 10
      record.getValue("dtg") must not(beNull)
      record.getValue("dtg") mustEqual feature.getAttribute("dtg")
      record.getValue("geom") mustEqual "POINT (45 55)"
    }

    "create a converter based on a record schema" in {
      val schema = {
        val id = new StandardSchemaIdentifier.Builder().name("test").build()
        val fields =
          Seq(
            new RecordField("id", RecordFieldType.STRING.getDataType),
            new RecordField("name", RecordFieldType.STRING.getDataType),
            new RecordField("age", RecordFieldType.INT.getDataType),
            new RecordField("dtg", RecordFieldType.DATE.getDataType),
            new RecordField("geom", RecordFieldType.STRING.getDataType),
            new RecordField("json", RecordFieldType.STRING.getDataType)
          )
        new SimpleRecordSchema(fields.asJava, id)
      }

      val opts = RecordConverterOptions(
        fidField = Some("id"),
        geomFields = Seq(GeometryColumn("geom", classOf[Point], default = true)),
        jsonFields = Seq("json")
      )
      val converter = SimpleFeatureRecordConverter(schema, opts)
      converter.sft.getTypeName mustEqual "test"
      converter.sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual
          Seq("name", "age", "dtg", "geom", "json")
      converter.sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[String], classOf[Integer], classOf[Date], classOf[Point], classOf[String])
      converter.sft.getDescriptor("json").isJson must beTrue

      val record = new MapRecord(schema, new java.util.HashMap[String, AnyRef])
      record.setValue("id", "id")
      record.setValue("name", "myname")
      record.setValue("age", 10)
      record.setValue("dtg", FastConverter.convert("2020-01-01T00:00:00.000Z", classOf[Date]))
      record.setValue("geom", "POINT (45 55)")
      record.setValue("json", "{}")

      val feature = converter.convert(record)
      feature.getID mustEqual "id"
      feature.getAttribute("name") mustEqual "myname"
      feature.getAttribute("age") mustEqual 10
      feature.getAttribute("dtg") must not(beNull)
      feature.getAttribute("dtg") mustEqual record.getValue("dtg")
      feature.getAttribute("geom") mustEqual WKTUtils.read("POINT (45 55)")
      feature.getAttribute("json") mustEqual "{}"
    }

    "convert nested records to json strings" in {
      val schema = {
        val id = new StandardSchemaIdentifier.Builder().name("test").build()
        val nested = Seq(
          new RecordField("height", RecordFieldType.DOUBLE.getDataType),
          new RecordField("weight", RecordFieldType.DOUBLE.getDataType),
          new RecordField("name", RecordFieldType.STRING.getDataType)
        )
        val fields =
          Seq(
            new RecordField("id", RecordFieldType.STRING.getDataType),
            new RecordField("dtg", RecordFieldType.DATE.getDataType),
            new RecordField("geom", RecordFieldType.STRING.getDataType),
            new RecordField("user", RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(nested.asJava)))
          )
        new SimpleRecordSchema(fields.asJava, id)
      }

      val opts = RecordConverterOptions(
        fidField = Some("id"),
        geomFields = Seq(GeometryColumn("geom", classOf[Point], default = true))
      )
      val converter = SimpleFeatureRecordConverter(schema, opts)
      converter.sft.getTypeName mustEqual "test"
      converter.sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("dtg", "geom", "user")
      converter.sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[Date], classOf[Point], classOf[String])
      converter.sft.getDescriptor("user").getUserData.get("json") mustEqual "true"

      val nested = new MapRecord(schema.getField("user").get.getDataType.asInstanceOf[RecordDataType].getChildSchema, new java.util.HashMap[String, AnyRef])
      nested.setValue("name", "myname")
      nested.setValue("height", 10.0)
      nested.setValue("weight", 30.0)

      val record = new MapRecord(schema, new java.util.HashMap[String, AnyRef])
      record.setValue("id", "id")
      record.setValue("dtg", FastConverter.convert("2020-01-01T00:00:00.000Z", classOf[Date]))
      record.setValue("geom", "POINT (45 55)")
      record.setValue("user", nested)

      val feature = converter.convert(record)
      feature.getID mustEqual "id"
      feature.getAttribute("dtg") must not(beNull)
      feature.getAttribute("dtg") mustEqual record.getValue("dtg")
      feature.getAttribute("geom") mustEqual WKTUtils.read("POINT (45 55)")
      feature.getAttribute("user") mustEqual """{"name":"myname","weight":30.0,"height":10.0}"""
    }

    "keep the feature ID as an attribute" in {
      val schema = {
        val id = new StandardSchemaIdentifier.Builder().name("test").build()
        val fields =
          Seq(
            new RecordField("name", RecordFieldType.STRING.getDataType),
            new RecordField("dtg", RecordFieldType.DATE.getDataType),
            new RecordField("geom", RecordFieldType.STRING.getDataType)
          )
        new SimpleRecordSchema(fields.asJava, id)
      }

      val opts = RecordConverterOptions(
        fidField = Some("name"),
        fidIsAttribute = true,
        geomFields = Seq(GeometryColumn("geom", classOf[Point], default = true))
      )
      val converter = SimpleFeatureRecordConverter(schema, opts)
      converter.sft.getTypeName mustEqual "test"
      converter.sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual
          Seq("name", "dtg", "geom")
      converter.sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[String], classOf[Date], classOf[Point])

      val record = new MapRecord(schema, new java.util.HashMap[String, AnyRef])
      record.setValue("name", "myname")
      record.setValue("dtg", FastConverter.convert("2020-01-01T00:00:00.000Z", classOf[Date]))
      record.setValue("geom", "POINT (45 55)")

      val feature = converter.convert(record)
      feature.getID mustEqual "myname"
      feature.getAttribute("name") mustEqual "myname"
      feature.getAttribute("dtg") must not(beNull)
      feature.getAttribute("dtg") mustEqual record.getValue("dtg")
      feature.getAttribute("geom") mustEqual WKTUtils.read("POINT (45 55)")
    }
  }
}
