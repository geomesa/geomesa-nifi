/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.records

import java.math.BigInteger
import java.util.{Date, UUID}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.google.gson.GsonBuilder
import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record._
import org.apache.nifi.serialization.record.`type`.{ArrayDataType, ChoiceDataType, MapDataType, RecordDataType}
import org.geomesa.nifi.datastore.processor.records.SimpleFeatureRecordConverter.FieldConverter
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
 * Converts between simple features and nifi records
 *
 * @param sft simple feature type
 * @param schema record schema
 * @param converters converters
 * @param fidField feature id record field name
 * @param visibilityField visibility record field name
 * @param userDataField user data record field name
 */
class SimpleFeatureRecordConverter(
    val sft: SimpleFeatureType,
    val schema: RecordSchema,
    converters: Array[FieldConverter[AnyRef, AnyRef]],
    fidField: Option[String],
    visibilityField: Option[String],
    userDataField: Option[String]) {

  import SimpleFeatureRecordConverter.gson
  import org.locationtech.geomesa.security.SecureSimpleFeature

  private val length = converters.length + fidField.size + visibilityField.size

  /**
   * Convert a feature to a record
   *
   * @param feature feature
   * @return
   */
  def convert(feature: SimpleFeature): Record = {
    val values = new java.util.LinkedHashMap[String, AnyRef](length)

    var i = 0
    while (i < converters.length) {
      val attribute = feature.getAttribute(i)
      if (attribute != null) {
        values.put(converters(i).name, converters(i).convertToRecord(attribute))
      }
      i += 1
    }

    fidField.foreach(values.put(_, feature.getID))
    for { f <- visibilityField; v <- feature.visibility } {
      values.put(f, v)
    }
    userDataField.foreach(values.put(_, gson.toJson(feature.getUserData)))

    new MapRecord(schema, values, false, false)
  }

  /**
   * Convert a record to a feature
   *
   * @param record record
   * @return
   */
  def convert(record: Record): SimpleFeature = {
    val feature = new ScalaSimpleFeature(sft, "")
    var i = 0
    while (i < converters.length) {
      val value = record.getValue(converters(i).name)
      if (value != null) {
        feature.setAttributeNoConvert(i, converters(i).convertToAttribute(value))
      }
      i += 1
    }
    fidField.foreach { field =>
      val fid = record.getAsString(field)
      if (fid != null) {
        feature.setId(fid)
        feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      }
    }
    visibilityField.foreach { name =>
      val vis = record.getAsString(name)
      if (vis != null) {
        feature.visibility = vis
      }
    }
    userDataField.foreach { name =>
      val json = record.getAsString(name)
      if (json != null) {
        feature.getUserData.putAll(gson.fromJson(json, classOf[java.util.Map[String, AnyRef]]))
      }
    }

    feature
  }
}

object SimpleFeatureRecordConverter extends LazyLogging {

  import org.locationtech.geomesa.utils.conversions.JavaConverters.OptionalToScala
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  private val gson = new GsonBuilder().serializeNulls().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").create()

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[(RecordSchema, RecordConverterOptions), Either[Throwable, SimpleFeatureRecordConverter]] {
      override def load(
          key: (RecordSchema, RecordConverterOptions)): Either[Throwable, SimpleFeatureRecordConverter] = {
        try {
          val (schema, options) = key
          Right(createConverterFromSchema(schema, options))
        } catch {
          case NonFatal(e) => Left(e)
        }
      }
    }
  )

  /**
   * Create a converter based on a feature type (useful for creating records from features)
   *
   * @param sft simple feature type
   * @return
   */
  def apply(sft: SimpleFeatureType): SimpleFeatureRecordConverter = apply(sft, SimpleFeatureConverterOptions())

  /**
   * Create a converter based on a feature type (useful for creating records from features)
   *
   * @param sft simple feature type
   * @param options converter options
   * @return
   */
  def apply(sft: SimpleFeatureType, options: SimpleFeatureConverterOptions): SimpleFeatureRecordConverter = {
    val converters = sft.getAttributeDescriptors.asScala.map { descriptor =>
      val name = descriptor.getLocalName
      if (classOf[Geometry].isAssignableFrom(descriptor.getType.getBinding)) {
        val converter = options.encoding match {
          case GeometryEncoding.Wkt => new GeometryWktFieldConverter(name, descriptor.getType.getBinding)
          case GeometryEncoding.Wkb => new GeometryWkbFieldConverter(name, descriptor.getType.getBinding)
          case _ => throw new NotImplementedError(s"Geometry encoding ${options.encoding}")
        }
        converter.asInstanceOf[FieldConverter[AnyRef, AnyRef]]
      } else {
        getConverter(name, ObjectType.selectType(descriptor))
      }
    }
    val id = new StandardSchemaIdentifier.Builder().name(sft.getTypeName).build()
    val idField = options.fidField.map(name => new RecordField(name, RecordFieldType.STRING.getDataType))
    val userDataField = options.userDataField.map(name => new RecordField(name, RecordFieldType.STRING.getDataType))
    val visField = options.visField.map(name => new RecordField(name, RecordFieldType.STRING.getDataType))
    val fields = idField.toSeq ++ converters.map(_.field) ++ userDataField ++ visField
    val schema = new SimpleRecordSchema(fields.asJava, id)
    schema.setSchemaName(sft.getTypeName) // seem to be two separate identifiers??

    new SimpleFeatureRecordConverter(sft, schema, converters.toArray, options.fidField, options.visField, options.userDataField)
  }

  /**
   * Create a converter based on a record schema (useful for creating features from records)
   *
   * @param schema record schema
   * @return
   */
  def apply(schema: RecordSchema): SimpleFeatureRecordConverter = apply(schema, RecordConverterOptions())

  /**
   * Create a converter based on a record schema (useful for creating features from records)
   *
   * @param schema record schema
   * @param options conversion options
   * @return
   */
  def apply(schema: RecordSchema, options: RecordConverterOptions): SimpleFeatureRecordConverter = {
    cache.get((schema, options)) match {
      case Right(c) => c
      case Left(e)  => throw e
      case null     => throw new RuntimeException("Unexpected error creating record converter")
    }
  }

  /**
   * Create a new converter
   *
   * @param schema schema
   * @param options options
   * @return
   */
  private def createConverterFromSchema(
      schema: RecordSchema,
      options: RecordConverterOptions): SimpleFeatureRecordConverter = {
    val typeName =
      options.typeName
          .orElse(schema.getSchemaName.asScala)
          .orElse(schema.getIdentifier.getName.asScala)
          .getOrElse(throw new IllegalArgumentException("No schema name defined in schema or processor"))

    // validate options
    val opts =
      options.fidField.toSeq ++
          options.geomFields.map(_.name) ++
          options.jsonFields ++
          options.dtgField ++
          options.visField

    opts.foreach { name =>
      if (!schema.getField(name).isPresent) {
        logger.warn(
          s"Schema does not contain configured field '$name': " +
              schema.getFieldNames.asScala.mkString(", "))
      }
    }

    val converters = schema.getFields.asScala.flatMap { field =>
      val name = field.getFieldName
      if (options.fidField.contains(name) || options.visField.contains(name)) {
        Seq.empty
      } else {
        options.geomFields.find(_.name == name) match {
          case Some(geom) =>
            val converter = field.getDataType match {
              case d if d == RecordDataTypes.StringType => new GeometryWktFieldConverter(name, geom.binding)
              case d if d == RecordDataTypes.BytesType  => new GeometryWkbFieldConverter(name, geom.binding)
              case d =>
                throw new IllegalArgumentException(
                  s"Invalid field type '$d' for geometry field $name, expected String or Byte Array")
            }
            Seq(converter.asInstanceOf[FieldConverter[AnyRef, AnyRef]])

          case None =>
            getConverter(name, field.getDataType).toSeq
        }
      }
    }

    val sft: SimpleFeatureType = {
      val builder = new SimpleFeatureTypeBuilder()
      builder.setName(typeName)
      val descriptors = converters.map { c =>
        val descriptor = c.descriptor
        if (options.jsonFields.contains(c.name)) {
          descriptor.getUserData.put(AttributeOptions.OptJson, "true")
        }
        descriptor
      }
      builder.addAll(descriptors.asJava)
      options.geomFields.find(_.default).foreach(g => builder.setDefaultGeometry(g.name))
      builder.buildFeatureType()
    }

    options.dtgField.foreach(sft.setDtgField)
    sft.getUserData.putAll(options.userData.asJava)

    val fidField = options.fidField
    val visField = options.visField

    new SimpleFeatureRecordConverter(sft, schema, converters.toArray, fidField, visField, None)
  }

  /**
   * Get a converter based on an attribute descriptor
   *
   * @param name attribute name
   * @param bindings type bindings
   * @return
   */
  private def getConverter(name: String, bindings: Seq[ObjectType]): FieldConverter[AnyRef, AnyRef] = {
    val converter = bindings.head match {
      case ObjectType.STRING   => new StringFieldConverter(name)
      case ObjectType.INT      => new IntFieldConverter(name)
      case ObjectType.LONG     => new LongFieldConverter(name)
      case ObjectType.FLOAT    => new FloatFieldConverter(name)
      case ObjectType.DOUBLE   => new DoubleFieldConverter(name)
      case ObjectType.BOOLEAN  => new BooleanFieldConverter(name)
      case ObjectType.DATE     => new DateFieldConverter(name)
      case ObjectType.UUID     => new UuidFieldConverter(name)
      case ObjectType.BYTES    => new BytesFieldConverter(name)
      case ObjectType.LIST     => new ListFieldConverter(name, getConverter("", bindings.tail))
      case ObjectType.MAP      => new MapFieldConverter(name, getConverter("", bindings.drop(2)))
      case b => throw new NotImplementedError(s"Unexpected attribute type: $b")
    }
    converter.asInstanceOf[FieldConverter[AnyRef, AnyRef]]
  }

  /**
   * Get a converter based on a record field
   *
   * @param name field name
   * @param dataType data type
   * @return
   */
  private def getConverter(
      name: String,
      dataType: DataType,
      path: Seq[String] = Seq.empty): Option[FieldConverter[AnyRef, AnyRef]] = {
    val converter = dataType.getFieldType match {
      case RecordFieldType.STRING    => Some(new StringFieldConverter(name))
      case RecordFieldType.BOOLEAN   => Some(new BooleanFieldConverter(name))
      case RecordFieldType.BYTE      => Some(new ByteFieldConverter(name))
      case RecordFieldType.SHORT     => Some(new ShortFieldConverter(name))
      case RecordFieldType.INT       => Some(new IntFieldConverter(name))
      case RecordFieldType.LONG      => Some(new LongFieldConverter(name))
      case RecordFieldType.BIGINT    => Some(new BigIntFieldConverter(name))
      case RecordFieldType.FLOAT     => Some(new FloatFieldConverter(name))
      case RecordFieldType.DOUBLE    => Some(new DoubleFieldConverter(name))
      case RecordFieldType.TIMESTAMP => Some(new DateFieldConverter(name, dataType))
      case RecordFieldType.DATE      => Some(new DateFieldConverter(name, dataType))
      case RecordFieldType.CHAR      => Some(new CharFieldConverter(name))

      case RecordFieldType.ARRAY =>
        val subType = dataType.asInstanceOf[ArrayDataType].getElementType
        if (subType.getFieldType == RecordFieldType.BYTE) {
          Some(new BytesFieldConverter(name))
        } else {
          getConverter("", subType, path ++ Seq(name)).map(new ListFieldConverter(name, _))
        }

      case RecordFieldType.MAP =>
        val subType = dataType.asInstanceOf[MapDataType].getValueType
        getConverter("", subType, path ++ Seq(name)).map(new MapFieldConverter(name, _))

      case RecordFieldType.CHOICE =>
        // TODO apply smarter logic on the widest common type (i.e. could convert int+long to long)
        Some(new ChoiceFieldConverter(name, dataType.asInstanceOf[ChoiceDataType].getPossibleSubTypes))

      case RecordFieldType.RECORD =>
        Some(new RecordFieldConverter(name, dataType.asInstanceOf[RecordDataType].getChildSchema))

      case t =>
        logger.warn(s"Dropping unsupported record field '${(path :+ name).mkString(".")}' of type: $t")
        None
    }
    converter.asInstanceOf[Option[FieldConverter[AnyRef, AnyRef]]]
  }

  trait FieldConverter[T, U] {
    def name: String
    def field: RecordField
    def descriptor: AttributeDescriptor
    def convertToRecord(value: T): U
    def convertToAttribute(value: U): T
  }

  trait IdentityFieldConverter[T] extends FieldConverter[T, T] {
    def convertToRecord(value: T): T = value
    def convertToAttribute(value: T): T = value
  }

  abstract class AbstractFieldConverter[T : ClassTag, U](val name: String, dataType: DataType)
      extends FieldConverter[T, U] {
    override val field: RecordField = new RecordField(name, dataType)
    override val descriptor: AttributeDescriptor =
      new AttributeTypeBuilder().binding(implicitly[ClassTag[T]].runtimeClass).buildDescriptor(name)
  }

  class StringFieldConverter(name: String)
      extends AbstractFieldConverter[String, String](name, RecordDataTypes.StringType)
        with IdentityFieldConverter[String]

  class IntFieldConverter(name: String)
      extends AbstractFieldConverter[Integer, Integer](name, RecordDataTypes.IntType)
          with IdentityFieldConverter[Integer]

  class LongFieldConverter(name: String)
      extends AbstractFieldConverter[java.lang.Long, java.lang.Long](name, RecordDataTypes.LongType)
          with IdentityFieldConverter[java.lang.Long]

  class FloatFieldConverter(name: String)
      extends AbstractFieldConverter[java.lang.Float, java.lang.Float](name, RecordDataTypes.FloatType)
          with IdentityFieldConverter[java.lang.Float]

  class DoubleFieldConverter(name: String)
      extends AbstractFieldConverter[java.lang.Double, java.lang.Double](name, RecordDataTypes.DoubleType)
          with IdentityFieldConverter[java.lang.Double]

  class BooleanFieldConverter(name: String)
      extends AbstractFieldConverter[java.lang.Boolean, java.lang.Boolean](name, RecordDataTypes.BooleanType)
          with IdentityFieldConverter[java.lang.Boolean]

  class DateFieldConverter(name: String, dataType: DataType = RecordDataTypes.DateType)
      extends AbstractFieldConverter[Date, Date](name, dataType)
          with IdentityFieldConverter[Date]

  class BytesFieldConverter(name: String)
      extends AbstractFieldConverter[Array[Byte], AnyRef](name, RecordDataTypes.BytesType) {
    override def convertToRecord(value: Array[Byte]): AnyRef = value
    override def convertToAttribute(value: AnyRef): Array[Byte] = fromRecordBytes(value)
  }

  class UuidFieldConverter(name: String)
      extends AbstractFieldConverter[UUID, String](name, RecordDataTypes.StringType) {
    override def convertToAttribute(value: String): UUID = UUID.fromString(value)
    override def convertToRecord(value: UUID): String = value.toString
  }

  class GeometryWktFieldConverter(val name: String, binding: Class[_])
      extends FieldConverter[Geometry, String] {
    override val field: RecordField = new RecordField(name, RecordDataTypes.StringType)
    override val descriptor: AttributeDescriptor = new AttributeTypeBuilder().binding(binding).buildDescriptor(name)
    override def convertToAttribute(value: String): Geometry = WKTUtils.read(value)
    override def convertToRecord(value: Geometry): String = WKTUtils.write(value)
  }

  class GeometryWkbFieldConverter(val name: String, binding: Class[_])
      extends FieldConverter[Geometry, AnyRef] {
    override val field: RecordField = new RecordField(name, RecordDataTypes.BytesType)
    override val descriptor: AttributeDescriptor = new AttributeTypeBuilder().binding(binding).buildDescriptor(name)
    override def convertToAttribute(value: AnyRef): Geometry = WKBUtils.read(fromRecordBytes(value))
    override def convertToRecord(value: Geometry): AnyRef = WKBUtils.write(value)
  }

  class ByteFieldConverter(name: String)
      extends AbstractFieldConverter[Integer, java.lang.Byte](name, RecordFieldType.BYTE.getDataType) {
    override def convertToRecord(value: Integer): java.lang.Byte = value.byteValue()
    override def convertToAttribute(value: java.lang.Byte): Integer = value.intValue()
  }

  class ShortFieldConverter(name: String)
      extends AbstractFieldConverter[Integer, java.lang.Short](name, RecordFieldType.SHORT.getDataType) {
    override def convertToRecord(value: Integer): java.lang.Short = value.shortValue()
    override def convertToAttribute(value: java.lang.Short): Integer = value.intValue()
  }

  class BigIntFieldConverter(name: String)
      extends AbstractFieldConverter[java.lang.Long, BigInteger](name, RecordFieldType.BIGINT.getDataType) {
    override def convertToRecord(value: java.lang.Long): BigInteger = BigInteger.valueOf(value)
    override def convertToAttribute(value: BigInteger): java.lang.Long = value.longValueExact()
  }

  class CharFieldConverter(name: String)
      extends AbstractFieldConverter[String, java.lang.Character](name, RecordFieldType.CHAR.getDataType) {
    override def convertToRecord(value: String): Character = value.charAt(0)
    override def convertToAttribute(value: Character): String = value.toString
  }

  class ListFieldConverter(val name: String, sub: FieldConverter[AnyRef, AnyRef])
      extends FieldConverter[java.util.List[AnyRef], Array[AnyRef]] {

    override val field: RecordField =
      new RecordField(name, RecordFieldType.ARRAY.getArrayDataType(sub.field.getDataType))

    override val descriptor: AttributeDescriptor =
      new AttributeTypeBuilder()
          .binding(classOf[java.util.List[AnyRef]])
          .buildDescriptor(name)
          .setListType(sub.descriptor.getType.getBinding)

    override def convertToRecord(value: java.util.List[AnyRef]): Array[AnyRef] =
      value.asScala.collect { case v if v != null => sub.convertToRecord(v) }.toArray

    override def convertToAttribute(value: Array[AnyRef]): java.util.List[AnyRef] =
      java.util.Arrays.asList(value.collect { case v if v != null => sub.convertToAttribute(v) }: _*)
  }

  // TODO records do not support non-string map keys
  class MapFieldConverter(val name: String, valueConverter: FieldConverter[AnyRef, AnyRef])
      extends FieldConverter[java.util.Map[AnyRef, AnyRef], java.util.Map[String, AnyRef]] {

    override val field: RecordField =
      new RecordField(name, RecordFieldType.MAP.getMapDataType(valueConverter.field.getDataType))

    override val descriptor: AttributeDescriptor =
      new AttributeTypeBuilder()
          .binding(classOf[java.util.Map[AnyRef, AnyRef]])
          .buildDescriptor(name)
          .setMapTypes(classOf[String], valueConverter.descriptor.getType.getBinding)

    override def convertToRecord(value: java.util.Map[AnyRef, AnyRef]): java.util.Map[String, AnyRef] =
      value.asScala.collect { case (k, v) if v != null => k.toString -> valueConverter.convertToRecord(v) }.asJava


    override def convertToAttribute(value: java.util.Map[String, AnyRef]): java.util.Map[AnyRef, AnyRef] = {
      val map = value.asScala.collect { case (k, v) if v != null => k -> valueConverter.convertToAttribute(v) }
      map.asJava.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
    }
  }

  class RecordFieldConverter(val name: String, schema: RecordSchema) extends FieldConverter[String, Record] {

    override val field: RecordField = new RecordField(name, RecordFieldType.RECORD.getRecordDataType(schema))

    override val descriptor: AttributeDescriptor =
      new AttributeTypeBuilder().binding(classOf[String]).userData("json", "true").buildDescriptor(name)

    override def convertToRecord(value: String): Record =
      throw new NotImplementedError("Record field converters are only implemented for record to feature")

    override def convertToAttribute(value: Record): String = gson.toJson(value.toMap)
  }

  class ChoiceFieldConverter(val name: String, choices: java.util.List[DataType])
      extends FieldConverter[String, AnyRef] {

    override val field: RecordField = new RecordField(name, RecordFieldType.CHOICE.getChoiceDataType(choices))

    override val descriptor: AttributeDescriptor =
      new AttributeTypeBuilder().binding(classOf[String]).buildDescriptor(name)

    override def convertToRecord(value: String): AnyRef =
      throw new NotImplementedError("Choice converters are only implemented for record to feature")

    override def convertToAttribute(value: AnyRef): String = value.toString
  }
}
