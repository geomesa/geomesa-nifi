/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.records

import java.io.OutputStream
import java.util

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.OnEnabled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.{AbstractControllerService, ConfigurationContext}
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.schema.access.SchemaNameAsAttribute
import org.apache.nifi.serialization.record.{Record, RecordSchema}
import org.apache.nifi.serialization.{AbstractRecordSetWriter, RecordSetWriterFactory}
import org.geomesa.nifi.datastore.processor.records.GeoAvroRecordSetWriterFactory.{GeoAvroRecordSetWriter, Props}
import org.locationtech.geomesa.features.avro.AvroDataFileWriter

import scala.collection.JavaConverters._

@Tags(Array("avro", "geoavro", "result", "set", "recordset", "record", "writer", "serializer", "row"))
@CapabilityDescription("Writes the contents of a RecordSet as GeoAvro which AvroToPutGeoMesa* Processors can use.")
class GeoAvroRecordSetWriterFactory extends AbstractControllerService with RecordSetWriterFactory {

  private var options: OptionExtractor = _

  // NB: This is the same as what the InheritSchemaFromRecord Strategy does
  override def getSchema(map: util.Map[String, String], recordSchema: RecordSchema): RecordSchema = recordSchema

  override def createWriter(
      logger: ComponentLog,
      recordSchema: RecordSchema,
      outputStream: OutputStream,
      variables: java.util.Map[String, String]): GeoAvroRecordSetWriter = {
    val opts = options.apply(getConfigurationContext, variables)
    new GeoAvroRecordSetWriter(recordSchema, opts, outputStream)
  }

  override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = Props

  @OnEnabled
  def setContext(context: ConfigurationContext): Unit = {
    options = OptionExtractor(context, GeometryEncoding.Wkb)
  }
}

object GeoAvroRecordSetWriterFactory {

  private val Props = Seq(
    Properties.TypeName,
    Properties.FeatureIdCol,
    Properties.GeometryCols,
    Properties.GeometrySerializationDefaultWkb,
    Properties.JsonCols,
    Properties.DefaultDateCol,
    Properties.VisibilitiesCol,
    Properties.SchemaUserData
  ).toList.asJava

  class GeoAvroRecordSetWriter(
      recordSchema: RecordSchema,
      options: RecordConverterOptions,
      outputStream: OutputStream
    ) extends AbstractRecordSetWriter(outputStream) {

    private val schemaAccessWriter = new SchemaNameAsAttribute()
    private val converter = SimpleFeatureRecordConverter(recordSchema, options)
    private val writer = new AvroDataFileWriter(outputStream, converter.sft)

    override def writeRecord(record: Record): util.Map[String, String] = {
      val sf = converter.convert(record)
      writer.append(sf)

      // This is the same as the CSV/XML RecordWriters.
      // It seems like this could be memo-ized.  Those writers do not, so we are not.
      schemaAccessWriter.getAttributes(recordSchema)
    }

    override def getMimeType: String = "application/avro-binary"

    override def close(): Unit = writer.close()
  }
}
