/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import org.apache.commons.io.FilenameUtils
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior._
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.mixins.ConvertInputProcessor
import org.geomesa.nifi.datastore.processor.mixins.ConvertInputProcessor.ConverterCallback
import org.geomesa.nifi.datastore.processor.validators.GzipLevelValidator
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.SerializationOption
import org.locationtech.geomesa.features.exporters.FileSystemExporter.{OrcFileSystemExporter, ParquetFileSystemExporter}
import org.locationtech.geomesa.features.exporters._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils}

import java.io.{File, OutputStream}
import java.nio.file.Files
import java.util.UUID
import java.util.zip.GZIPOutputStream
import scala.util.control.NonFatal

@Tags(Array("OGC", "geo", "convert", "converter", "simple feature", "geotools", "geomesa",
  "parquet", "orc", "arrow", "gml", "geojson", "csv", "leaflet"))
@CapabilityDescription("Convert incoming files into OGC data files using GeoMesa Converters")
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes(
  Array(
    new ReadsAttribute(attribute = "geomesa.converter", description = "GeoMesa converter name or configuration"),
    new ReadsAttribute(attribute = "geomesa.sft.name", description = "GeoMesa SimpleFeatureType name"),
    new ReadsAttribute(attribute = "geomesa.sft.spec", description = "GeoMesa SimpleFeatureType specification")
  )
)
@WritesAttributes(
  Array(
    new WritesAttribute(attribute = "geomesa.convert.successes", description = "Number of features written successfully"),
    new WritesAttribute(attribute = "geomesa.convert.failures", description = "Number of features with errors")
  )
)
@SupportsBatching
class ConvertToGeoFile extends ConvertInputProcessor {

  import ConvertToGeoFile.Properties.{GzipLevel, IncludeHeaders, OutputFormat}

  override protected def getShips: Seq[Relationship] =
    super.getShips ++ Seq(Relationships.OriginalRelationship)

  override protected def getPrimaryProperties: Seq[PropertyDescriptor] =
    super.getPrimaryProperties ++ Seq(OutputFormat, GzipLevel, IncludeHeaders)

  override protected def getTertiaryProperties: Seq[PropertyDescriptor] =
    super.getTertiaryProperties ++ Seq(ExtraClasspaths)

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    var input = session.get()
    if (input == null) {
      return
    }
    var output: FlowFile = null
    try {
      val sft = loadFeatureType(context, input)
      val format = context.getProperty(OutputFormat).getValue
      val gzip = Option(context.getProperty(GzipLevel).evaluateAttributeExpressions().getValue).map(_.toInt)
      val headers = Option(context.getProperty(IncludeHeaders).getValue).forall(_.toBoolean)

      output = session.create(input)

      var result: IngestResult = null // have to update the attributes after closing the output file

      def export(exporter: FeatureExporter): Unit = {
        try {
          exporter.start(sft)
          val callback = new ConverterCallback() {
            override def apply(features: Iterator[SimpleFeature]): Long = {
              exporter.export(features)
              0L
            }
          }
          result = convert(context, session, input, sft, callback)
        } finally {
          CloseWithLogging(exporter)
        }
      }

      if (format == "orc" || format == "parquet") {
        val dir = Files.createTempDirectory("gm-nifi")
        try {
          val file = new File(dir.toFile, s"export.$format")
          val exporter = format match {
            case "orc"     => new OrcFileSystemExporter(file.getAbsolutePath)
            case "parquet" => new ParquetFileSystemExporter(file.getAbsolutePath)
            // case ExportFormat.Shp     => new ShapefileExporter(file) // TODO zip up dir??
            // shouldn't happen unless someone adds a new format and doesn't implement it here
            case _ => throw new NotImplementedError(s"Export for '$format' is not implemented")
          }
          export(exporter)
          output = session.importFrom(file.toPath, false, output)
        } finally {
          PathUtils.deleteRecursively(dir)
        }
      } else {
        output = session.write(output, new OutputStreamCallback() {
          override def process(out: OutputStream): Unit = {
            // note: avro handles compression internally
            lazy val stream = gzip match {
              case None    => out
              case Some(c) => new GZIPOutputStream(out) { `def`.setLevel(c) } // hack to access the protected deflate level
            }
            val exporter = format match {
              case "arrow"       => new ArrowExporter(stream, ConvertToGeoFile.getArrowHints(sft))
              case "avro"        => new AvroExporter(out, gzip)
              case "avro-native" => new AvroExporter(out, gzip, Set(SerializationOption.NativeCollections))
              case "bin"         => new BinExporter(stream, new Hints())
              case "csv"         => DelimitedExporter.csv(stream, headers)
              case "gml"         => GmlExporter(stream)
              case "gml2"        => GmlExporter.gml2(stream)
              case "json"        => new GeoJsonExporter(stream)
              case "leaflet"     => new LeafletMapExporter(stream)
              case "tsv"         => DelimitedExporter.tsv(stream, headers)
              // shouldn't happen unless someone adds a new format and doesn't implement it here
              case _ => throw new NotImplementedError(s"Export for '$format' is not implemented")
            }
            export(exporter)
          }
        })
      }

      val basename =
        Option(input.getAttribute("filename")).map(FilenameUtils.getBaseName).getOrElse(UUID.randomUUID().toString)
      val ext = format match {
        case "avro-native"  => "avro"
        case "gml" | "gml2" => "xml"
        case "leaflet"      => "html"
        case _              => format
      }
      output = session.putAttribute(output, "filename", s"$basename.$ext")
      output = session.removeAttribute(output, "mime.type")

      val attributes = new java.util.HashMap[String, String](2)
      attributes.put("geomesa.convert.successes", result.success.toString)
      attributes.put("geomesa.convert.failures", result.failure.toString)

      output = session.putAllAttributes(output, attributes)
      input = session.putAllAttributes(input, attributes)

      if (result.success > 0L) {
        session.transfer(output, Relationships.SuccessRelationship)
        session.transfer(input, Relationships.OriginalRelationship)
      } else {
        session.remove(output)
        session.transfer(input, Relationships.FailureRelationship)
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error converting file: ${e.getMessage}", e)
        session.transfer(input, Relationships.FailureRelationship)
        if (output != null) {
          session.remove(output)
        }
    }
  }
}

object ConvertToGeoFile {

  import scala.collection.JavaConverters._

  val Formats: Seq[String] =
    Seq(
      "arrow",
      "avro",
      "avro-native",
      "bin",
      "csv",
      "gml2",
      "gml",
      "json",
      "leaflet",
      "orc",
      "parquet",
      // TODO ExportFormat.Shp,
      "tsv",
    )

  /**
   * Gets the hints to inform an arrow export.
   *
   * Currently we:
   *   <ul>
   *     <li>exclude fids</li>
   *     <li>dictionary encode all string fields</li>
   *     <li>TODO sort by date</li>
   *   </ul>
   *
   * @param sft simple feature type
   * @return
   */
  private def getArrowHints(sft: SimpleFeatureType): Hints = {
    val hints = new Hints()
    hints.put(QueryHints.ARROW_INCLUDE_FID, java.lang.Boolean.FALSE)
    // TODO dictionaries + sorting is not supported in the arrow exporter
    // sft.getDtgField.foreach(hints.put(QueryHints.ARROW_SORT_FIELD, _))
    val dictionaries = sft.getAttributeDescriptors.asScala.collect {
      case d if d.getType.getBinding == classOf[String] => d.getLocalName
    }
    if (dictionaries.nonEmpty) {
      hints.put(QueryHints.ARROW_DICTIONARY_FIELDS, dictionaries.mkString(","))
    }
    hints
  }

  object Properties {
    val OutputFormat: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("output-format")
          .displayName("Output format")
          .description("File format for the outgoing simple feature file")
          .required(true)
          .allowableValues(Formats: _*)
          .defaultValue("avro")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build()

    val GzipLevel: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("gzip-level")
          .displayName("GZIP level")
          .description("Level of gzip compression to apply to output, from 1-9")
          .required(false)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .addValidator(GzipLevelValidator)
          .build()

    val IncludeHeaders: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("include-headers")
          .displayName("Include headers")
          .description("Include header line in delimited export formats")
          .required(false)
          .allowableValues("true", "false")
          .defaultValue("true")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build()
  }
}
