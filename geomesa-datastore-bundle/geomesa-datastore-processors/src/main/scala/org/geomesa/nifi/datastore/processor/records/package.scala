/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.{DataType, RecordFieldType}
import org.geomesa.nifi.datastore.processor.records.Properties._
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec.GeomAttributeSpec
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser
import org.locationtech.jts.geom.Geometry

import java.io.StringReader
import java.util.{Collections, Properties}
import scala.util.Try
import scala.util.control.NonFatal

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

     val FeatureIdIsAttribute: PropertyDescriptor =
      new PropertyDescriptor.Builder()
        .name("feature-id-is-attribute")
        .displayName("Keep Feature ID as attribute")
        .description("Keep the Feature ID column as an attribute in the feature type")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("false")
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
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
         .defaultValue("${geomesa.sft.user-data}")
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
      fidIsAttribute: Boolean = false,
      geomFields: Seq[GeometryColumn] = Seq.empty,
      encoding: GeometryEncoding.GeometryEncoding = GeometryEncoding.Wkt,
      jsonFields: Seq[String] = Seq.empty,
      dtgField: Option[String] = None,
      visField: Option[String] = None,
      userData: Map[String, AnyRef] = Map.empty
    ) extends ConverterOptions

  case class GeometryColumn(name: String, binding: Class[_ <: Geometry], default: Boolean)

  sealed trait OptionExtractor {

    /**
     * Create converter options based on the environment
     *
     * @param context process/session context
     * @param variables flow file attributes
     * @return
     */
    def apply(
        context: PropertyContext,
        variables: java.util.Map[String, String] = Collections.emptyMap()): RecordConverterOptions
  }

  object OptionExtractor {

    import GeometryEncoding.GeometryEncoding

    /**
     * Create an option extractor for the given context
     *
     * @param context context
     * @param encoding default geometry encoding
     * @return
     */
    def apply(context: PropertyContext, encoding: GeometryEncoding): OptionExtractor = {

      val typeName = TypeNameExtractor.static(context)
      val fidCol = FeatureIdExtractor.static(context)
      val fidIsAttribute = FidIsAttributeExtractor.static(context)
      val geomCols = GeometryColsExtractor.static(context)
      val geomEncoding = encoding match {
        case GeometryEncoding.Wkt => GeometryEncodingWktExtractor.static(context)
        case GeometryEncoding.Wkb => GeometryEncodingWkbExtractor.static(context)
      }
      val jsonCols = JsonColumnsExtractor.static(context)
      val dtgCol = DefaultDateExtractor.static(context)
      val visCol = VisibilitiesExtractor.static(context)
      val userData = UserDataExtractor.static(context)

      val dynamic =
        new DynamicConfiguration(typeName, fidCol, fidIsAttribute, geomCols, geomEncoding, jsonCols, dtgCol, visCol, userData)

      val static =
        Seq(typeName, fidCol, fidIsAttribute, geomCols, geomEncoding, jsonCols, dtgCol, visCol, userData)
            .forall(_.isInstanceOf[StaticEvaluation[_]])

      if (!static) { dynamic } else {
        new StaticConfiguration(dynamic.apply(context, Collections.emptyMap()))
      }
    }

    /**
     * A static configuration that doesn't depend on the environment (i.e. no expression language)
     *
     * @param opts opts
     */
    class StaticConfiguration(opts: RecordConverterOptions) extends OptionExtractor {
      override def apply(
          context: PropertyContext,
          variables: java.util.Map[String, String]): RecordConverterOptions = opts
    }

    /**
     * A dynamic configuration that depends on the environment (i.e. has expression language)
     *
     * Each individual config value may be static so it's not re-evaluated each time
     */
    class DynamicConfiguration(
        typeName: PropertyExtractor[Option[String]],
        fidCol: PropertyExtractor[Option[String]],
        fidIsAttribute: PropertyExtractor[Option[String]],
        geomCols: PropertyExtractor[Seq[GeometryColumn]],
        geomEncoding: PropertyExtractor[GeometryEncoding],
        jsonCols: PropertyExtractor[Seq[String]],
        dtgCol: PropertyExtractor[Option[String]],
        visCol: PropertyExtractor[Option[String]],
        userData: PropertyExtractor[Map[String, AnyRef]]
      ) extends OptionExtractor {

      override def apply(
          context: PropertyContext,
          variables: java.util.Map[String, String]): RecordConverterOptions = {
        RecordConverterOptions(
          typeName       = typeName.apply(context, variables),
          fidField       = fidCol.apply(context, variables),
          fidIsAttribute = fidIsAttribute.apply(context, variables).exists(_.toBoolean),
          geomFields     = geomCols.apply(context, variables),
          encoding       = geomEncoding.apply(context, variables),
          jsonFields     = jsonCols.apply(context, variables),
          dtgField       = dtgCol.apply(context, variables),
          visField       = visCol.apply(context, variables),
          userData       = userData.apply(context, variables)
        )
      }
    }

    /**
     * Gets the value for a single property descriptor
     *
     * @tparam T type
     */
    private sealed trait PropertyExtractor[T] {

      /**
       * Evaluate the property descriptor
       *
       * @param context context
       * @param variables flow file variables
       * @return
       */
      def apply(context: PropertyContext, variables: java.util.Map[String, String]): T

      /**
       * Make the extractor static (memoize it), if possible
       *
       * @param context context
       * @return
       */
      def static(context: PropertyContext): PropertyExtractor[T]
    }

    private class StaticEvaluation[T](value: T) extends PropertyExtractor[T] {
      override def apply(context: PropertyContext, variables: java.util.Map[String, String]): T = value
      override def static(context: PropertyContext): PropertyExtractor[T] = this
    }

    private abstract class DynamicEvaluation[T](prop: PropertyDescriptor) extends PropertyExtractor[T] {
      override def apply(context: PropertyContext, variables: java.util.Map[String, String]): T = {
        val p = context.getProperty(prop)
        val value = if (!p.isSet) { null } else {
          Option(p.evaluateAttributeExpressions(variables).getValue).filter(_.nonEmpty).orNull
        }
        wrap(value)
      }
      override def static(context: PropertyContext): PropertyExtractor[T] = {
        val p = context.getProperty(prop)
        if (p.isSet && p.isExpressionLanguagePresent) { this } else {
          new StaticEvaluation(apply(context, Collections.emptyMap()))
        }
      }

      protected def wrap(value: String): T
    }

    private class OptionEvaluation(prop: PropertyDescriptor) extends DynamicEvaluation[Option[String]](prop) {
      override def wrap(value: String): Option[String] = Option(value)
    }

    private object TypeNameExtractor extends OptionEvaluation(TypeName)
    private object FeatureIdExtractor extends OptionEvaluation(FeatureIdCol)
    private object FidIsAttributeExtractor extends OptionEvaluation(FeatureIdIsAttribute)
    private object DefaultDateExtractor extends OptionEvaluation(DefaultDateCol)
    private object VisibilitiesExtractor extends OptionEvaluation(VisibilitiesCol)

    private object GeometryColsExtractor extends DynamicEvaluation[Seq[GeometryColumn]](GeometryCols) {
      override protected def wrap(value: String): Seq[GeometryColumn] = {
        Option(value).toSeq.flatMap { spec =>
          SimpleFeatureSpecParser.parse(spec).attributes.collect {
            case g: GeomAttributeSpec => GeometryColumn(g.name, g.clazz, g.default)
          }
        }
      }
    }

    private object GeometryEncodingWktExtractor
        extends DynamicEvaluation[GeometryEncoding](GeometrySerializationDefaultWkt) {
      override protected def wrap(value: String): GeometryEncoding = GeometryEncoding(value)
    }

    private object GeometryEncodingWkbExtractor
        extends DynamicEvaluation[GeometryEncoding](GeometrySerializationDefaultWkb) {
      override protected def wrap(value: String): GeometryEncoding = GeometryEncoding(value)
    }

    private object JsonColumnsExtractor extends DynamicEvaluation[Seq[String]](JsonCols) {
      override protected def wrap(value: String): Seq[String] =
        Option(value).toSeq.flatMap(_.split(",")).map(_.trim())
    }

    private object UserDataExtractor extends DynamicEvaluation[Map[String, AnyRef]](SchemaUserData) {

      import scala.collection.JavaConverters._

      override protected def wrap(value: String): Map[String, AnyRef] = {
        value match {
          case null | "" => Map.empty[String, AnyRef]
          case spec =>
            // note: we have a bit of a disconnect with how user data is parsed in records (as a spec string) vs
            // in the other processors (as a java properties format). this makes some sense since records are
            // row-oriented, but here we add the props parsing as a fall-back to make things consistent
            try { parseSpec(spec) } catch {
              case NonFatal(e) => Try(parseProps(spec)).getOrElse(throw e) // rethrow the original exception
            }
        }
      }

      private def parseSpec(userData: String): Map[String, AnyRef] =
        SimpleFeatureSpecParser.parse(";" + userData).options

      private def parseProps(userData: String): Map[String, AnyRef] = {
        val props = new Properties()
        props.load(new StringReader(userData))
        props.asInstanceOf[java.util.Map[String, String]].asScala.toMap
      }
    }
  }
}
