/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import java.io.InputStream

import org.apache.nifi.annotation.behavior.{SupportsBatching, WritesAttribute, WritesAttributes}
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.{ProcessContext, ProcessSession}
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.Record
import org.geomesa.nifi.datastore.processor.RecordUpdateProcessor.{AttributeFilter, FidFilter}
import org.geomesa.nifi.datastore.processor.mixins.DataStoreProcessor
import org.geomesa.nifi.datastore.processor.records.{RecordConverterOptions, SimpleFeatureRecordConverter}
import org.geotools.data.{DataUtilities, Transaction}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.annotation.tailrec
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Processor for updating certain attributes of a feature. As compared to the other ingest processors in
 * 'modify' mode (which require the entire feature to be input), this processor only requires specifying
 * the fields that will be updated
 */
@SupportsBatching
@WritesAttributes(
  Array(
    new WritesAttribute(attribute = "geomesa.ingest.successes", description = "Number of features updated successfully"),
    new WritesAttribute(attribute = "geomesa.ingest.failures", description = "Number of features with errors")
  )
)
@CapabilityDescription("Update existing features in GeoMesa")
trait RecordUpdateProcessor extends DataStoreProcessor {

  import RecordUpdateProcessor.Properties.LookupCol
  import org.geomesa.nifi.datastore.processor.mixins.Properties.NifiBatchSize
  import org.geomesa.nifi.datastore.processor.records.Properties.RecordReader
  import org.locationtech.geomesa.security.SecureSimpleFeature

  import scala.collection.JavaConverters._

  override protected def getPrimaryProperties: Seq[PropertyDescriptor] =
    super.getPrimaryProperties ++ RecordUpdateProcessor.Props

  override protected def getConfigProperties: Seq[PropertyDescriptor] =
    super.getConfigProperties ++ Seq(NifiBatchSize)

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val flowFiles = session.get(context.getProperty(NifiBatchSize).evaluateAttributeExpressions().asInteger())
    logger.debug(s"Processing ${flowFiles.size()} files in batch")
    if (flowFiles != null && !flowFiles.isEmpty) {
      flowFiles.asScala.foreach { file =>
        val fullFlowFileName = fullName(file)
        try {
          logger.debug(s"Running ${getClass.getName} on file $fullFlowFileName")
          val props = loadProperties(context, file)
          val ds = getCachedDataStore(props.dsParams)
          val factory = props.services(RecordReader).asInstanceOf[RecordReaderFactory]
          val options = RecordConverterOptions(props.properties)
          val id = props.properties(LookupCol)
          val filterFactory = if (options.fidField.contains(id)) { FidFilter } else { new AttributeFilter(id) }

          var success, failure = 0L

          session.read(file, new InputStreamCallback {
            override def process(in: InputStream): Unit = {
              WithClose(factory.createRecordReader(file, in, logger)) { reader =>
                val converter = SimpleFeatureRecordConverter(reader.getSchema, options)
                val typeName = converter.sft.getTypeName

                val existing = Try(ds.getSchema(typeName)).getOrElse(null)
                if (existing == null) {
                  throw new IllegalStateException(s"Schema '$typeName' does not exist in the data store")
                }

                val names = converter.sft.getAttributeDescriptors.asScala.flatMap { d =>
                  val name = d.getLocalName
                  if (existing.indexOf(name) == -1) {
                    logger.warn(s"Attribute '$name' does not exist in the schema and will be ignored")
                    None
                  } else {
                    Some(name)
                  }
                }

                @tailrec
                def nextRecord: Record = {
                  try {
                    return reader.nextRecord()
                  } catch {
                    case NonFatal(e) =>
                      failure += 1L
                      logger.error("Error reading record from file", e)
                  }
                  nextRecord
                }

                var record = nextRecord
                while (record != null) {
                  try {
                    val sf = converter.convert(record)
                    val filter = filterFactory(sf)
                    try {
                      WithClose(ds.getFeatureWriter(typeName, filter, Transaction.AUTO_COMMIT)) { writer =>
                        if (!writer.hasNext) {
                          logger.warn(s"Filter does not match any features, skipping update: ${filterToString(filter)}")
                          failure += 1L
                        } else {
                          do {
                            val toWrite = writer.next()
                            names.foreach(n => toWrite.setAttribute(n, sf.getAttribute(n)))
                            if (options.fidField.isDefined) {
                              toWrite.getUserData.put(Hints.PROVIDED_FID, sf.getID)
                            }
                            if (options.visField.isDefined) {
                              sf.visibility.foreach(toWrite.visibility = _)
                            }
                            writer.write()
                            success += 1L
                          } while (writer.hasNext)
                        }
                      }
                    } catch {
                      case NonFatal(e) =>
                        failure += 1L
                        logger.error(s"Error writing feature to store: '${DataUtilities.encodeFeature(sf)}'", e)
                    }
                  } catch {
                    case NonFatal(e) =>
                      failure += 1L
                      logger.error(s"Error converting record to feature: '${record.toMap.asScala.mkString(",")}'", e)
                  }
                  record = nextRecord
                }
              }
            }
          })

          var output = file
          output = session.putAttribute(output, Attributes.IngestSuccessCount, success.toString)
          output = session.putAttribute(output, Attributes.IngestFailureCount, failure.toString)
          session.transfer(output, Relationships.SuccessRelationship)

          logger.debug(s"Ingested file $fullFlowFileName with $success successes and $failure failures")
        } catch {
          case NonFatal(e) =>
            logger.error(s"Error processing file $fullFlowFileName:", e)
            session.transfer(file, Relationships.FailureRelationship)
        }
      }
    }
  }
}

object RecordUpdateProcessor {

  import RecordUpdateProcessor.Properties.LookupCol
  import org.geomesa.nifi.datastore.processor.records.Properties._
  import org.locationtech.geomesa.filter.FilterHelper.ff

  private val Props = Seq(
    RecordReader,
    TypeName,
    LookupCol,
    FeatureIdCol,
    GeometryCols,
    GeometrySerializationDefaultWkt,
    VisibilitiesCol
  )

  sealed trait QueryFilter {
    def apply(f: SimpleFeature): Filter
  }

  object FidFilter extends QueryFilter {
    override def apply(f: SimpleFeature): Filter = ff.id(ff.featureId(f.getID))
  }

  class AttributeFilter(name: String) extends QueryFilter {
    private val prop = ff.property(name)
    override def apply(f: SimpleFeature): Filter = ff.equals(prop, ff.literal(f.getAttribute(name)))
  }

  object Properties {
    val LookupCol: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("lookup-col")
          .displayName("Lookup column")
          .description("Column that will be used to match features for update")
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
          .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
          .required(true)
          .build()
  }
}
