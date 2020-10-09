/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.{DataType, RecordFieldType}
import org.locationtech.jts.geom.Geometry

package object records {

  object Properties {

    val RecordReader: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record reader")
        .description("The Record Reader to use for deserializing the incoming data")
        .identifiesControllerService(classOf[RecordReaderFactory])
        .required(true)
        .build

    val TypeName: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("feature-type-name")
        .displayName("Feature type name")
        .description("Name to use for the simple feature type schema")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val FeatureIdCol: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("feature-id-col")
        .displayName("Feature ID column")
        .description("Column that will be used as the feature ID")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val GeometryCols: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("geometry-cols")
        .displayName("Geometry columns")
        .description(
          "Column(s) that will be deserialized as geometries and their type, as a SimpleFeatureType " +
            "specification string. A '*' can be used to indicate the default geometry column")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val GeometrySerializationDefaultWkt: PropertyDescriptor =
      buildGeometrySerializationProperty(GeometryEncodingLabels.Wkt)

    val GeometrySerializationDefaultWkb: PropertyDescriptor =
      buildGeometrySerializationProperty(GeometryEncodingLabels.Wkb)

    val JsonCols: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("json-cols")
        .displayName("JSON columns")
        .description(
          "Column(s) that contain valid JSON documents, comma-separated. " +
            "The columns must be STRING type")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val DefaultDateCol: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("default-date-col")
        .displayName("Default date column")
        .description("Column to use as the default date attribute")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val VisibilitiesCol: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("visibilities-col")
        .displayName("Visibilities column")
        .description("Column to use for the feature visibilities")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val SchemaUserData: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("schema-user-data")
        .displayName("Schema user data")
        .description("User data used to configure the GeoMesa SimpleFeatureType, in the form 'key1=value1,key2=value2'")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()
  }

  private def buildGeometrySerializationProperty(defaultSerializationFormat: String) = {
    new PropertyDescriptor.Builder()
      .name("geometry-serialization")
      .displayName("Geometry Serialization Format")
      .description("The format to use for serializing/deserializing geometries")
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
      .allowableValues(GeometryEncodingLabels.Wkt, GeometryEncodingLabels.Wkb)
      .defaultValue(defaultSerializationFormat)
      .build()
  }

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
      visField: Option[String] = None,
      userDataField: Option[String] = None
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
