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
import org.geomesa.nifi.datastore.processor.records.GeoAvroRecordSetWriterFactory.Props
import org.geomesa.nifi.datastore.processor.records.Properties.{GeometryCols, TypeName, VisibilitiesCol}
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

  override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = Props
}

object GeoAvroRecordSetWriterFactory {
  private val Props = Seq(
    Properties.GeometryCols,
    Properties.VisibilitiesCol,
    Properties.TypeName,
    Properties.GeometrySerialization
  ).toList.asJava
}

class GeoAvroRecordSetWriter(componentLog: ComponentLog, recordSchema: RecordSchema, outputStream: OutputStream, map: util.Map[PropertyDescriptor, String]) extends AbstractRecordSetWriter(outputStream) {
  private val schemaAccessWriter = new SchemaNameAsAttribute()

  /// Get Conf
  private val recordConverterOptions = {
    def getEncodings(descriptorToString: util.Map[PropertyDescriptor, String], defaultEncoding: GeometryEncoding): Map[String, TypeAndEncoding] = {
      val geometryColumns = descriptorToString.get(GeometryCols)
      if (geometryColumns == null) {
        Map()
      } else {
        geometryColumns
          .split(",")
          .map { s =>
            // TODO: Make this exception better!
            val splits = s.split(":")
            if (splits.size < 2) throw new Exception(s"Improper configuration string: ${map.get(GeometryCols)}")
            val encoding = if (splits.size == 2) {
              defaultEncoding
            } else {
              GeometryEncoding(splits(2))
            }
            (splits(0), TypeAndEncoding(splits(1), encoding))
          }.toMap
      }
    }

    val encodings = getEncodings(map, GeometryEncoding.Wkt)
    val defaultGeometryColumn = Option(map.get(GeometryCols)).map(_.split(":")(0))
    val typeName = Option(map.get(TypeName))

    val geometryColumns= encodings.map { case (k, v) =>
      GeometryColumn(k, v.clazz, defaultGeometryColumn.isDefined && defaultGeometryColumn.get.equals(k))
    }.toSeq

    val visField = Some(map.get(VisibilitiesCol))

    RecordConverterOptions(typeName, None, geometryColumns, visField = visField)
  }

  private val converter = SimpleFeatureRecordConverter(recordSchema, recordConverterOptions)
  private val writer = new AvroDataFileWriter(outputStream, converter.sft)

  override def writeRecord(record: Record): util.Map[String, String] = {
    val sf = converter.convert(record)
    writer.append(sf)
    schemaAccessWriter.getAttributes(recordSchema)
  }

  override def getMimeType: String = "application/avro-binary"

  override def close(): Unit = writer.close()
}
