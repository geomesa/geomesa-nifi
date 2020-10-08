/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.serialization.record.{DataType, RecordFieldType}
import org.locationtech.jts.geom.Geometry

package object records {

  def fromRecordBytes(bytes: AnyRef): Array[Byte] = {
    bytes match {
      case b: Array[Byte] => b
      case b: Array[java.lang.Byte] => b.map(_.byteValue)
      case b: Array[AnyRef] => b.map(_.asInstanceOf[java.lang.Byte].byteValue)
      case _ => throw new IllegalStateException(s"Unexpected byte array type: ${bytes.getClass}")
    }
  }

  object GeometryEncoding extends Enumeration {

    type GeometryEncoding = Value
    val Wkt, Wkb = Value

    def apply(value: String): GeometryEncoding = {
      value match {
        case GeometryEncodingLabels.Wkt => Wkt
        case GeometryEncodingLabels.Wkb => Wkb
        case s => throw new IllegalArgumentException(s"Unexpected geometry encoding: $s")
      }
    }
  }

  object GeometryEncodingLabels {
    val Wkt = "WKT (well-known text)"
    val Wkb = "WKB (well-known binary)"
  }

  object RecordDataTypes {
    val StringType  : DataType = RecordFieldType.STRING.getDataType
    val IntType     : DataType = RecordFieldType.INT.getDataType
    val LongType    : DataType = RecordFieldType.LONG.getDataType
    val FloatType   : DataType = RecordFieldType.FLOAT.getDataType
    val DoubleType  : DataType = RecordFieldType.DOUBLE.getDataType
    val BooleanType : DataType = RecordFieldType.BOOLEAN.getDataType
    val DateType    : DataType = RecordFieldType.DATE.getDataType("yyyy-MM-dd'T'HH:mm:ssZ")
    val BytesType   : DataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType)
  }

  trait ConverterOptions {
    def fidField: Option[String]
    def encoding: GeometryEncoding.GeometryEncoding
    def visField: Option[String]
  }

  case class SimpleFeatureConverterOptions(
      fidField: Option[String] = Some("id"),
      encoding: GeometryEncoding.GeometryEncoding = GeometryEncoding.Wkt,
      visField: Option[String] = None
    ) extends ConverterOptions

  case class RecordConverterOptions(
      typeName: Option[String] = None,
      fidField: Option[String] = None,
      geomFields: Seq[GeometryColumn] = Seq.empty,
      encoding: GeometryEncoding.GeometryEncoding = GeometryEncoding.Wkt,
      jsonFields: Seq[String] = Seq.empty,
      dtgField: Option[String] = None,
      visField: Option[String] = None,
      userData: Map[String, AnyRef] = Map.empty
    ) extends ConverterOptions

  case class GeometryColumn(name: String, binding: Class[_ <: Geometry], default: Boolean)
}
