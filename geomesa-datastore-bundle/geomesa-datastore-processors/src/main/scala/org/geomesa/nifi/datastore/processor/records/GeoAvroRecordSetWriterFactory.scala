package org.geomesa.nifi.datastore.processor.records

import java.io.OutputStream
import java.util

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.schema.access.SchemaNameAsAttribute
import org.apache.nifi.serialization.record.{Record, RecordSchema}
import org.apache.nifi.serialization.{AbstractRecordSetWriter, RecordSetWriterFactory}
import org.geomesa.nifi.datastore.processor.records.GeoAvroRecordSetWriterFactory.{GeometryColumns, VisibilitiesColumn, TypeName}
import org.geomesa.nifi.datastore.processor.records.GeometryEncoding.GeometryEncoding
import org.geomesa.nifi.datastore.processor.records.SimpleFeatureRecordConverter.TypeAndEncoding
import org.locationtech.geomesa.features.avro.AvroDataFileWriter

import scala.collection.JavaConverters._

@Tags(Array("avro", "geoavro", "result", "set", "recordset", "record", "writer", "serializer", "row"))
@CapabilityDescription("Writes the contents of a RecordSet as GeoAvro which AvroToPutGeoMesa* Processors can use.")
class GeoAvroRecordSetWriterFactory extends AbstractControllerService with RecordSetWriterFactory {
  // NB: This is the same as what the InheritSchemaFromRecord Strategy does
  override def getSchema(map: util.Map[String, String], recordSchema: RecordSchema): RecordSchema = recordSchema

  override def createWriter(componentLog: ComponentLog,
                            recordSchema: RecordSchema,
                            outputStream: OutputStream,
                            map: util.Map[String, String]): GeoAvroRecordSetWriter = {
    new GeoAvroRecordSetWriter(componentLog, recordSchema, outputStream, getConfigurationContext.getProperties)
  }

  override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] =
    Seq(GeometryColumns, VisibilitiesColumn).toList.asJava
}

object GeoAvroRecordSetWriterFactory {
  // TODO factor out
  val VisibilitiesColumn: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("visibilities-column")
      .displayName("Visibilities Column")
      .description("The name of a column with visibility expressions for each row.  " +
        "If used in conjunction with the GetGeoMesaKafkaRecord processor, note that it can be configured to write out " +
        "visibilities to a column named \"visibilities\".")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .required(false).build

  val GeometryColumns: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("Geometry Columns")
    .description("Comma-separated list of columns with geometries with type. " +
      "Example: position:Point,line:LineString.  " +
      "Note the first field is used as a the default geometry.")
    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false).build

  val TypeName: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("SimpleFeature TypeName")
    .description("The type name for the output.")
    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false).build
}

class GeoAvroRecordSetWriter(componentLog: ComponentLog, recordSchema: RecordSchema, outputStream: OutputStream, map: util.Map[PropertyDescriptor, String]) extends AbstractRecordSetWriter(outputStream) {
  private val schemaAccessWriter = new SchemaNameAsAttribute()
  // Wkt is assumed to be the default GeometryEncoding.
  // TODO:  Extract this as a property?
  private val encodings = getEncodings(map, GeometryEncoding.Wkt)
  private val defaultGeometryColumn = Option(map.get(GeometryColumns)).map(_.split(":")(0))
  private val typeName = Option(map.get(TypeName))

  private val geometryColumns= encodings.map { case (k, v) =>
    GeometryColumn(k, v.clazz, defaultGeometryColumn.isDefined && defaultGeometryColumn.get.equals(k))
  }.toSeq

  private val visField = Some(map.get(VisibilitiesColumn))
  private val recordConverterOptions = RecordConverterOptions(typeName, None, geometryColumns, visField = visField)
  private val converter = SimpleFeatureRecordConverter(recordSchema, recordConverterOptions)

  private val sft = converter.sft
  private val writer = new AvroDataFileWriter(outputStream, sft)

  override def writeRecord(record: Record): util.Map[String, String] = {
    val sf = converter.convert(record)
    writer.append(sf)
    schemaAccessWriter.getAttributes(recordSchema)
  }

  private def getEncodings(descriptorToString: util.Map[PropertyDescriptor, String], defaultEncoding: GeometryEncoding): Map[String, TypeAndEncoding] = {
    val geometryColumns = descriptorToString.get(GeometryColumns)
    if (geometryColumns == null) {
      Map()
    } else {
      geometryColumns
        .split(",")
        .map { s =>
          // TODO: Make this exception better!
          val splits = s.split(":")
          if (splits.size < 2) throw new Exception(s"Improper configuration string: ${map.get(GeometryColumns)}")
          val encoding = if (splits.size == 2) {
            defaultEncoding
          } else {
            GeometryEncoding(splits(2))
          }
          (splits(0), TypeAndEncoding(splits(1), encoding))
        }.toMap
    }
  }

  override def getMimeType: String = "application/avro-binary"

  override def close(): Unit = writer.close()
}
