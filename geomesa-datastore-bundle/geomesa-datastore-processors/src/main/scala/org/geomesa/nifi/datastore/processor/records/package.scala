/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
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
import org.geomesa.nifi.datastore.processor.records.Properties._
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec.GeomAttributeSpec
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser
import org.locationtech.jts.geom.Geometry

package object records {

  object Expressions {

    val IdCol    = new RichExpression("geomesa.id.col")
    val GeomCols = new RichExpression("geomesa.geometry.cols")
    val JsonCols = new RichExpression("geomesa.json.cols")
    val DtgCol   = new RichExpression("geomesa.default.dtg.col")
    val VisCol   = new RichExpression("geomesa.visibilities.col")

    implicit class RichExpression(val attribute: String) extends AnyVal {
      def toExpression: String = "${" + attribute + "}"
    }
  }

  object Properties {

    val RecordReader: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record reader")
        .description("The record reader to use for deserializing incoming data")
        .identifiesControllerService(classOf[RecordReaderFactory])
        .required(true)
        .build

    val TypeName: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("feature-type-name")
        .displayName("Feature type name")
        .description("Name to use for the simple feature type schema")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val FeatureIdCol: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("feature-id-col")
        .displayName("Feature ID column")
        .description("Column that will be used as the feature ID")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(Expressions.IdCol.toExpression)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
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
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(Expressions.GeomCols.toExpression)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val GeometrySerializationDefaultWkt: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("geometry-serialization")
          .displayName("Geometry serialization format")
          .description(
            "The format to use for serializing/deserializing geometries - " +
                "either WKT (well-known text) or WKB (well-known binary)")
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
          .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
          .allowableValues("WKT", "WKB")
          .defaultValue("WKT")
          .build()

    val GeometrySerializationDefaultWkb: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .fromPropertyDescriptor(GeometrySerializationDefaultWkt)
          .defaultValue("WKB")
          .build()

    val JsonCols: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("json-cols")
        .displayName("JSON columns")
        .description(
          "Column(s) that contain valid JSON documents, comma-separated. " +
            "The columns must be STRING type")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(Expressions.JsonCols.toExpression)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val DefaultDateCol: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("default-date-col")
        .displayName("Default date column")
        .description("Column to use as the default date attribute")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(Expressions.DtgCol.toExpression)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val VisibilitiesCol: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("visibilities-col")
        .displayName("Visibilities column")
        .description("Column to use for the feature visibilities")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(Expressions.VisCol.toExpression)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
        .build()

    val SchemaUserData: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("schema-user-data")
        .displayName("Schema user data")
        .description("User data used to configure the GeoMesa SimpleFeatureType, in the form 'key1=value1,key2=value2'")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(false)
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
        case v if v.equalsIgnoreCase("wkt") => Wkt
        case v if v.equalsIgnoreCase("wkb") => Wkb
        case s => throw new IllegalArgumentException(s"Unexpected geometry encoding: $s")
      }
    }
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
      fidField: Option[String] = Some(SimpleFeatureRecordConverter.DefaultIdCol),
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

  object RecordConverterOptions {

    import GeometryEncoding.GeometryEncoding

    def apply(properties: Map[PropertyDescriptor, String]): RecordConverterOptions = {
      RecordConverterOptions(
        typeName   = TypeNameExtractor.apply(properties),
        fidField   = FeatureIdExtractor.apply(properties),
        geomFields = GeometryColsExtractor.apply(properties).getOrElse(Seq.empty),
        encoding   = GeometryEncodingExtractor.apply(properties),
        jsonFields = JsonColumnsExtractor.apply(properties).getOrElse(Seq.empty),
        dtgField   = DefaultDateExtractor.apply(properties),
        visField   = VisibilitiesExtractor.apply(properties),
        userData   = UserDataExtractor.apply(properties).getOrElse(Map.empty)
      )
    }

    /**
     * Gets the value for a single property descriptor
     *
     * @tparam T type
     */
    private abstract class PropertyExtractor[T <: AnyRef](prop: PropertyDescriptor) {

      /**
       * Evaluate the property descriptor
       *
       * @param properties evaluated properties from the processor and flow file
       * @return
       */
      def apply(properties: Map[PropertyDescriptor, String]): Option[T] =
        properties.get(prop).collect { case v if v.nonEmpty => wrap(v) }

      protected def wrap(value: String): T
    }

    private class OptionEvaluation(prop: PropertyDescriptor) extends PropertyExtractor[String](prop) {
      override def wrap(value: String): String = value
    }

    private object TypeNameExtractor extends OptionEvaluation(TypeName)
    private object FeatureIdExtractor extends OptionEvaluation(FeatureIdCol)
    private object DefaultDateExtractor extends OptionEvaluation(DefaultDateCol)
    private object VisibilitiesExtractor extends OptionEvaluation(VisibilitiesCol)

    private object GeometryColsExtractor extends PropertyExtractor[Seq[GeometryColumn]](GeometryCols) {
      override protected def wrap(value: String): Seq[GeometryColumn] = {
        SimpleFeatureSpecParser.parse(value).attributes.collect {
          case g: GeomAttributeSpec => GeometryColumn(g.name, g.clazz, g.default)
        }
      }
    }

    private object GeometryEncodingWktExtractor
        extends PropertyExtractor[GeometryEncoding](GeometrySerializationDefaultWkt) {
      override protected def wrap(value: String): GeometryEncoding = GeometryEncoding(value)
    }

    private object GeometryEncodingWkbExtractor
        extends PropertyExtractor[GeometryEncoding](GeometrySerializationDefaultWkb) {
      override protected def wrap(value: String): GeometryEncoding = GeometryEncoding(value)
    }

    private object GeometryEncodingExtractor {
      def apply(properties: Map[PropertyDescriptor, String]): GeometryEncoding =
        GeometryEncodingWktExtractor.apply(properties)
            .orElse(GeometryEncodingWkbExtractor.apply(properties))
            .getOrElse(throw new IllegalStateException("No default geometry encoding value found in processor"))
    }

    private object JsonColumnsExtractor extends PropertyExtractor[Seq[String]](JsonCols) {
      override protected def wrap(value: String): Seq[String] = value.split(",").map(_.trim())
    }

    private object UserDataExtractor extends PropertyExtractor[Map[String, AnyRef]](SchemaUserData) {
      override protected def wrap(value: String): Map[String, AnyRef] =
        SimpleFeatureSpecParser.parse(";" + value).options
    }

  }
}
