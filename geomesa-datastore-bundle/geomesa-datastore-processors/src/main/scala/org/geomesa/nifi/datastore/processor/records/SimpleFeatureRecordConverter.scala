/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.records

import java.util
import java.util.{Date, UUID}

import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record._
import org.geomesa.nifi.datastore.processor.records.GeometryEncoding.GeometryEncoding
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Converts between simple features and nifi records
 */
trait SimpleFeatureRecordConverter {
  def sft: SimpleFeatureType
  def schema: RecordSchema
  def convert(feature: SimpleFeature): Record
  def convert(feature: Record): SimpleFeature
}

object SimpleFeatureRecordConverter {

  import scala.collection.JavaConverters._

  def fromSFT(sft: SimpleFeatureType, encoding: GeometryEncoding = GeometryEncoding.Wkt): SimpleFeatureRecordConverter = {
    val converters = sft.getAttributeDescriptors.asScala.map { descriptor =>
      getConverter(descriptor.getLocalName, ObjectType.selectType(descriptor), encoding)
    }.toArray

    val schema: RecordSchema = getRecordSchema(sft, converters)
    new SimpleFeatureRecordConverterImpl(sft, schema, converters)
  }

  def fromRecordSchema(schema: RecordSchema,
                       encodings: scala.collection.Map[String, TypeAndEncoding],
                       encoding: GeometryEncoding = GeometryEncoding.Wkt,
                       defaultGeometryColumn: Option[String] = None,
                       typeName: Option[String] = None): SimpleFeatureRecordConverter = {
    val sft: SimpleFeatureType = recordSchemaToSFT(schema, encodings, defaultGeometryColumn, typeName)
    val converters = sft.getAttributeDescriptors.asScala.map { descriptor =>
      getConverter(descriptor.getLocalName, ObjectType.selectType(descriptor), encoding, encodings)
    }.toArray

    new SimpleFeatureRecordConverterImpl(sft, schema, converters)
  }

  private def recordSchemaToSFT(schema: RecordSchema,
                                encodings: scala.collection.Map[String, TypeAndEncoding],
                                defaultGeometryColumn: Option[String] = None,
                                typeName: Option[String] = None): SimpleFeatureType = {
    schema match {
      case sftSchema: SimpleFeatureTypeRecordSchema => sftSchema.sft
      case _ => 
        // If we do not have enough information, throw an exception
        if (encodings.isEmpty) {
          throw new Exception(s"No geometry columns were configured.")
        }
        val builder = new SimpleFeatureTypeBuilder

        schema.getFields.asScala.foreach { field =>
          val name = field.getFieldName

          encodings.get(name) match {
            case Some(geometryType) =>
              builder.add(name, geometryType.clazz)
            case None =>
              val clazz = dataTypeToClass(field.getDataType)
              builder.add(name, clazz)
          }
        }
        // The default geometry is set as the first column name.
        defaultGeometryColumn.foreach(builder.setDefaultGeometry)
        typeName.foreach(builder.setName)

        builder.buildFeatureType()
    }
  }

  private val geometryTypeMap = scala.collection.Map(
    "Geometry"           -> classOf[Geometry],
    "Point"              -> classOf[Point],
    "LineString"         -> classOf[LineString],
    "Polygon"            -> classOf[Polygon],
    "MultiPoint"         -> classOf[MultiPoint],
    "MultiLineString"    -> classOf[MultiLineString],
    "MultiPolygon"       -> classOf[MultiPolygon],
    "GeometryCollection" -> classOf[GeometryCollection]
  )
  
  private def dataTypeToClass(dataType: DataType): Class[_] = {
    dataType.getFieldType match {
      case RecordFieldType.BOOLEAN => classOf[Boolean]
      case RecordFieldType.DATE    => classOf[Date]
      case RecordFieldType.DOUBLE  => classOf[java.lang.Double]
      case RecordFieldType.FLOAT   => classOf[java.lang.Float]
      case RecordFieldType.INT     => classOf[java.lang.Integer]
      case RecordFieldType.LONG    => classOf[java.lang.Long]
      case RecordFieldType.STRING  => classOf[java.lang.String]
      case _ =>
        throw new Exception(s"Do not know how to map type $dataType to a SimpleFeatureType.")
      //        case RecordFieldType.ARRAY.getArrayDataType =>
      //        case RecordFieldType.MAP.getMapDataType =>
    }
  }


  private def getConverter(
      name: String,
      bindings: Seq[ObjectType],
      encoding: GeometryEncoding,
      encodings: scala.collection.Map[String, TypeAndEncoding] = new util.HashMap[String, TypeAndEncoding].asScala): AttributeFieldConverter[AnyRef, AnyRef] = {
    val converter = bindings.head match {
      case ObjectType.STRING   => new StringFieldConverter(name)
      case ObjectType.INT      => new IntFieldConverter(name)
      case ObjectType.LONG     => new LongFieldConverter(name)
      case ObjectType.FLOAT    => new FloatFieldConverter(name)
      case ObjectType.DOUBLE   => new DoubleFieldConverter(name)
      case ObjectType.BOOLEAN  => new BooleanFieldConverter(name)
      case ObjectType.DATE     => new DateFieldConverter(name)
      case ObjectType.UUID     => new UuidFieldConverter(name)
      case ObjectType.GEOMETRY => GeometryToRecordField(name, encoding, encodings)
      case ObjectType.LIST     => new ListToRecordField(name, getConverter("", bindings.tail, encoding, encodings))
      case ObjectType.MAP      => new MapToRecordField(name, getConverter("", bindings.drop(2), encoding, encodings))
      case ObjectType.BYTES    => new BytesToRecordField(name)
      case b => throw new NotImplementedError(s"Unexpected attribute type: $b")
    }
    converter.asInstanceOf[AttributeFieldConverter[AnyRef, AnyRef]]
  }

  private def bytesType: DataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType)

  class SimpleFeatureRecordConverterImpl(
      val sft: SimpleFeatureType,
      val schema: RecordSchema,
      converters: Array[AttributeFieldConverter[AnyRef, AnyRef]]
    ) extends SimpleFeatureRecordConverter {

    override def convert(feature: SimpleFeature): Record = {
      val values = new java.util.LinkedHashMap[String, AnyRef](converters.length + 1)
      values.put("id", feature.getID)
      var i = 0
      while (i < converters.length) {
        values.put(converters(i).field.getFieldName, converters(i).toRecord(feature.getAttribute(i)))
        i += 1
      }
      new SimpleFeatureMapRecord(feature, schema, values)
    }

    override def convert(feature: Record): SimpleFeature = {
      val raw = feature.getValues
      val values = Array.ofDim[AnyRef](converters.length)
      var i = 0
      while (i < converters.length) {
        // Why is this raw(i+1)?
        values(i) = converters(i).toAttribute(raw(i))
        i += 1
      }
      // TODO:  New FID attribute
      new ScalaSimpleFeature(sft, raw(0).toString, values)
    }
  }

  private def getRecordSchema(sft: SimpleFeatureType, converters: Array[AttributeFieldConverter[AnyRef, AnyRef]]) = {
     val fields = new util.ArrayList[RecordField](converters.length + 1)
    fields.add(FidConverter.field)
    converters.foreach(c => fields.add(c.field))
    val schema = new SimpleFeatureTypeRecordSchema(sft, fields)
    schema.setSchemaName(sft.getTypeName)
    schema
  }

  trait AttributeFieldConverter[T <: AnyRef, U <: AnyRef] {
    def field: RecordField
    def toRecord(attribute: T): U
    def toAttribute(record: U): T
  }

  class IdentityFieldConverter[T <: AnyRef](name: String, dataType: DataType) extends AttributeFieldConverter[T, T] {
    override val field: RecordField = new RecordField(name, dataType)
    override def toRecord(value: T): T = value
    override def toAttribute(value: T): T = value
  }

  object FidConverter extends IdentityFieldConverter[String]("id", RecordFieldType.STRING.getDataType)

  class StringFieldConverter(name: String)
      extends IdentityFieldConverter[String](name, RecordFieldType.STRING.getDataType)

  class IntFieldConverter(name: String)
      extends IdentityFieldConverter[Integer](name, RecordFieldType.INT.getDataType)

  class LongFieldConverter(name: String)
      extends IdentityFieldConverter[java.lang.Long](name, RecordFieldType.LONG.getDataType)

  class FloatFieldConverter(name: String)
      extends IdentityFieldConverter[java.lang.Float](name, RecordFieldType.FLOAT.getDataType)

  class DoubleFieldConverter(name: String)
      extends IdentityFieldConverter[java.lang.Double](name, RecordFieldType.DOUBLE.getDataType)

  class BooleanFieldConverter(name: String)
      extends IdentityFieldConverter[java.lang.Boolean](name, RecordFieldType.BOOLEAN.getDataType)

  class DateFieldConverter(name: String)
      extends IdentityFieldConverter[Date](name, RecordFieldType.DATE.getDataType("yyyy-MM-dd'T'HH:mm:ssZ"))

  class UuidFieldConverter(name: String) extends AttributeFieldConverter[UUID, String] {
    override val field: RecordField = new RecordField(name, RecordFieldType.STRING.getDataType)
    override def toRecord(attribute: UUID): String = if (attribute == null) { null } else { attribute.toString }
    override def toAttribute(record: String): UUID = if (record == null) { null } else { UUID.fromString(record) }
  }

  object GeometryToRecordField {
    def apply(name: String, encoding: GeometryEncoding, encodings: scala.collection.Map[String, TypeAndEncoding]): AttributeFieldConverter[Geometry, _] = {
      // Look up encoding in PropertyDescriptors and then fall back to the encoding passed in.
      encodings.get(name).map(_.encoding).getOrElse(encoding) match {
        case GeometryEncoding.Wkt => new GeometryToWktRecordField(name)
        case GeometryEncoding.Wkb => new GeometryToWkbRecordField(name)
        case _ => throw new NotImplementedError(s"Geometry encoding $encoding")
      }
    }
  }

  case class TypeAndEncoding(clazz: Class[_], encoding: GeometryEncoding)

  object TypeAndEncoding {
    def apply(clazzString: String, encoding: GeometryEncoding): TypeAndEncoding = {
      TypeAndEncoding(geometryTypeMap(clazzString), encoding)
    }
  }

  class GeometryToWktRecordField(name: String) extends AttributeFieldConverter[Geometry, String] {
    override val field: RecordField = new RecordField(name, RecordFieldType.STRING.getDataType)
    override def toRecord(attribute: Geometry): String = if (attribute == null) { null } else { WKTUtils.write(attribute) }
    override def toAttribute(record: String): Geometry = if (record == null) { null } else { WKTUtils.read(record) }
  }

  class GeometryToWkbRecordField(name: String) extends AttributeFieldConverter[Geometry, Array[Byte]] {
    override val field: RecordField = new RecordField(name, bytesType)
    override def toRecord(attribute: Geometry): Array[Byte] = if (attribute == null) { null } else { WKBUtils.write(attribute) }
    override def toAttribute(record: Array[Byte]): Geometry = if (record == null) { null } else { WKBUtils.read(record) }
  }

  class ListToRecordField(name: String, subType: AttributeFieldConverter[AnyRef, AnyRef])
      extends AttributeFieldConverter[java.util.List[AnyRef], Array[AnyRef]] {
    override val field: RecordField = new RecordField(name, RecordFieldType.ARRAY.getArrayDataType(subType.field.getDataType))
    override def toRecord(attribute: java.util.List[AnyRef]): Array[AnyRef] =
      if (attribute == null) { null } else { attribute.asScala.map(subType.toRecord).toArray }
    override def toAttribute(record: Array[AnyRef]): java.util.List[AnyRef] =
      if (record == null) { null } else { java.util.Arrays.asList(record.map(subType.toAttribute): _*) }
  }

  class MapToRecordField(name: String, valueType: AttributeFieldConverter[AnyRef, AnyRef])
      extends AttributeFieldConverter[java.util.Map[AnyRef, AnyRef], java.util.Map[String, AnyRef]] {
    override val field: RecordField =
      new RecordField(name, RecordFieldType.MAP.getMapDataType(valueType.field.getDataType))
    override def toRecord(attribute: java.util.Map[AnyRef, AnyRef]): java.util.Map[String, AnyRef] = {
      if (attribute == null) { null } else {
        attribute.asScala.map { case (k, v) => k.toString -> valueType.toRecord(v) }.asJava
      }
    }
    override def toAttribute(record: java.util.Map[String, AnyRef]): java.util.Map[AnyRef, AnyRef] = {
      if (record == null) { null } else {
        // TODO records do not support non-string map keys
        record.asScala.map { case (k, v) => k -> valueType.toAttribute(v) }.asJava.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      }
    }
  }

  class BytesToRecordField(name: String) extends IdentityFieldConverter(name, bytesType)
}

class SimpleFeatureTypeRecordSchema(val sft: SimpleFeatureType, fields: util.List[RecordField])
  extends SimpleRecordSchema(fields: util.List[RecordField])

class SimpleFeatureMapRecord(val sf: SimpleFeature, schema: RecordSchema, values: util.Map[String, AnyRef])
  extends MapRecord(schema, values, false, false)
