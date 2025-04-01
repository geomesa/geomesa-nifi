/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor


import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior._
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.CompatibilityMode.CompatibilityMode
import org.geomesa.nifi.datastore.processor.mixins.{DataStoreIngestProcessor, FeatureTypeProcessor, FeatureWriters}
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.util.Converters
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.io.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import java.io.InputStream
import scala.util.Try
import scala.util.control.NonFatal

@Tags(Array("geomesa", "geo", "ingest", "avro", "geotools"))
@CapabilityDescription("Ingest GeoAvro files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes(
  Array(
    new ReadsAttribute(attribute = "geomesa.sft.name", description = "GeoMesa SimpleFeatureType name"),
    new ReadsAttribute(attribute = "geomesa.sft.spec", description = "GeoMesa SimpleFeatureType specification")
  )
)
@WritesAttributes(
  Array(
    new WritesAttribute(attribute = "geomesa.ingest.successes", description = "Number of features written successfully"),
    new WritesAttribute(attribute = "geomesa.ingest.failures", description = "Number of features with errors")
  )
)
@SupportsBatching
class AvroToPutGeoMesa extends DataStoreIngestProcessor with FeatureTypeProcessor {

  import AvroToPutGeoMesa.Properties.UseProvidedFid

  override protected def getSecondaryProperties: Seq[PropertyDescriptor] =
    Seq(UseProvidedFid) ++ super.getSecondaryProperties ++ Seq(ExtraClasspaths)

  // noinspection ScalaDeprecation
  override protected def createIngest(
      context: ProcessContext,
      service: DataStoreService,
      writers: FeatureWriters,
      mode: CompatibilityMode): IngestProcessor = {
    val useProvidedFid = context.getProperty(UseProvidedFid).getValue.toBoolean
    new AvroIngest(service, writers, mode, useProvidedFid)
  }

  /**
   * GeoAvro ingest
   *
   * @param service data store service
   * @param writers feature writers
   * @param mode field match mode
   * @param useProvidedFid use provided fid
   */
  class AvroIngest(
      service: DataStoreService,
      writers: FeatureWriters,
      mode: CompatibilityMode,
      useProvidedFid: Boolean
    ) extends IngestProcessor(service, writers, mode) {

    private val store = service.loadDataStore()

    override def ingest(
        context: ProcessContext,
        session: ProcessSession,
        file: FlowFile,
        fileName: String): IngestResult = {

      var success = 0L
      var failure = 0L

      val nameArg = loadFeatureTypeName(context, file)
      val configuredSft = loadFeatureTypeSpec(context, file).map(s => loadFeatureType(context, file, Some(s), nameArg))
      // create/update the schema if it's configured in the processor or flow-file attributes
      configuredSft.foreach(checkSchema)

      session.read(file, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          val reader = new AvroDataFileReader(in)
          try {
            val fileSft = {
              val readerSft = reader.getSft
              nameArg.orElse(configuredSft.map(_.getTypeName)) match {
                case Some(name) if readerSft.getTypeName != name => SimpleFeatureTypes.renameSft(readerSft, name)
                case _ => readerSft
              }
            }

            // note: if we have a configured sft, we've already accounted for the update above
            val shouldUpdate = configuredSft.isEmpty
            val features = checkSchemaAndMapping(fileSft, shouldUpdate) match {
              case None => reader
              case Some(m) => reader.map(m.apply)
            }

            writers.borrow(fileSft.getTypeName, file) { writer =>
              features.foreach { sf =>
                try {
                  if (useProvidedFid) {
                    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
                  }
                  writer.apply(sf)
                  success += 1L
                } catch {
                  case NonFatal(e) =>
                    failure += 1L
                    logError(sf, e)
                }
              }
            }
          } finally {
            reader.close()
          }
        }
      })

      IngestResult(success, failure)
    }

    override def close(): Unit = {
      try { service.dispose(store) } finally {
        super.close()
      }
    }

    private def checkSchemaAndMapping(
        sft: SimpleFeatureType,
        shouldUpdate: Boolean): Option[SimpleFeature => SimpleFeature] = {
      val existing = Try(store.getSchema(sft.getTypeName)).getOrElse(null)

      if (existing == null) {
        logger.info(s"Creating schema ${sft.getTypeName}: ${SimpleFeatureTypes.encodeType(sft)}")
        store.createSchema(sft)
        None
      } else if (SimpleFeatureTypes.compare(sft, existing) == 0) {
        None
      } else if (mode == CompatibilityMode.Update && shouldUpdate) {
        logger.info(
          s"Updating schema ${sft.getTypeName}:" +
              s"\n  from: ${SimpleFeatureTypes.encodeType(existing)}" +
              s"\n  to:   ${SimpleFeatureTypes.encodeType(sft)}")
        store.updateSchema(sft.getTypeName, sft)
        None
      } else if (mode == CompatibilityMode.Existing || mode == CompatibilityMode.Update ) {
        Some(AvroToPutGeoMesa.convert(sft, existing))
      } else {
        throw new IllegalArgumentException(
          "Input schema does not match existing type:" +
              s"\n  Input:    ${SimpleFeatureTypes.encodeType(sft)}" +
              s"\n  Existing: ${SimpleFeatureTypes.encodeType(existing)}")
      }
    }
  }
}

object AvroToPutGeoMesa {

  import scala.collection.JavaConverters._

  object Properties {

    val UseProvidedFid: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("Use provided feature ID")
          .description("Use the feature ID from the Avro record, or generate a new one")
          .required(false)
          .allowableValues("true", "false")
          .defaultValue("true")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build()
  }

  /**
   * Creates an adapter from one SFT to another
   *
   * @param in input sft
   * @param out output sft
   * @return
   */
  def convert(in: SimpleFeatureType, out: SimpleFeatureType): SimpleFeature => SimpleFeature = {
    val outGeometryLocalName = Option(out.getGeometryDescriptor).map(_.getLocalName).orNull
    val inToOut: Seq[SimpleFeature => AnyRef] = out.getAttributeDescriptors.asScala.toSeq.map { outAttributeDescriptor =>
      val outLocalName = outAttributeDescriptor.getLocalName
      in.indexOf(outLocalName) match {
        case inIndex if inIndex >= 0 =>
          val outTypeBinding = outAttributeDescriptor.getType.getBinding
          if (outTypeBinding.isAssignableFrom(in.getType(inIndex).getBinding)) {
            sf: SimpleFeature => sf.getAttribute(inIndex)
          } else {
            sf: SimpleFeature => Converters.convert(sf.getAttribute(inIndex), outTypeBinding).asInstanceOf[AnyRef]
          }
        case _ if outLocalName == outGeometryLocalName => sf: SimpleFeature => sf.getDefaultGeometry
        case _ => _: SimpleFeature => null
      }
    }
    sf: SimpleFeature => {
      val o = ScalaSimpleFeature.create(out, sf.getID, inToOut.map(_.apply(sf)): _*)
      o.getUserData.putAll(sf.getUserData)
      o
    }
  }
}

