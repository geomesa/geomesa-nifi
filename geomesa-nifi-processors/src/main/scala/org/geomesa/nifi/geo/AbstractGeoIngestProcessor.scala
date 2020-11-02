/***********************************************************************
 * Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.geo

import java.io.InputStream

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, ObjectPool, PooledObject}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Relationships._
import org.geomesa.nifi.geo.AvroIngest.{AvroMatchMode, LenientMatch, UseProvidedFid, convert}
import org.geomesa.nifi.geo.validators.{ConverterValidator, SimpleFeatureTypeValidator}
import org.geotools.data.{DataStore, DataUtilities, FeatureWriter, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs, SimpleFeatureTypeLoader}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._

abstract class AbstractGeoIngestProcessor extends AbstractProcessor {

  type ProcessFn = (ProcessContext, ProcessSession, FlowFile) => Unit
  type SFW       = FeatureWriter[SimpleFeatureType, SimpleFeature]
  type ToStream  = (String, InputStream) => Iterator[SimpleFeature] with AutoCloseable

  protected var descriptors: java.util.List[PropertyDescriptor] = null
  protected var relationships: java.util.Set[Relationship] = null

  protected override def init(context: ProcessorInitializationContext): Unit = {
    relationships = Set(SuccessRelationship, FailureRelationship).asJava
    descriptors = List(
      IngestModeProp,
      SftName,
      ConverterName,
      FeatureNameOverride,
      SftSpec,
      ConverterSpec,
      NifiBatchSize,
      UseProvidedFid,
      AvroMatchMode
    ).asJava
  }

  override def getRelationships = relationships
  override def getSupportedPropertyDescriptors = descriptors

  @volatile
  protected var converterPool: ObjectPool[SimpleFeatureConverter[_]] = _

  @volatile
  protected var sft: SimpleFeatureType = null

  @volatile
  protected var ingestMode: String = null

  @volatile
  protected var dataStore: DataStore = null

  @volatile
  protected var matchMode: String = null

  @OnScheduled
  protected def initialize(context: ProcessContext): Unit = {
    // Data store comes first...then getSft because
    // oddly enough sometimes you want to modify the sft
    dataStore = getDataStore(context)
    require(dataStore != null, "Fatal error datastore is null")
    sft = getSft(context)

    createTypeIfNeeded(this.dataStore, this.sft)

    ingestMode = context.getProperty(IngestModeProp).getValue
    if(IngestMode.Converter == ingestMode) {
      initializeConverterPool(context)
    }
    matchMode = context.getProperty(AvroMatchMode).getValue
    getLogger.info(s"Initialized datastore ${dataStore.getClass.getSimpleName} with SFT ${sft.getTypeName} in mode $ingestMode")
  }

  private def initializeConverterPool(context: ProcessContext) = {
    val convertArg = Option(context.getProperty(ConverterName).getValue)
      .orElse(Option(context.getProperty(ConverterSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${ConverterName.getName} or ${ConverterSpec.getName} property"))
    val config = ConverterConfigResolver.getArg(ConfArgs(convertArg)) match {
      case Left(e) => throw e
      case Right(conf) => conf
    }

    converterPool = new GenericObjectPool[SimpleFeatureConverter[_]](
      new BasePooledObjectFactory[SimpleFeatureConverter[_]] {
        override def create(): SimpleFeatureConverter[_] = SimpleFeatureConverters.build(sft, config)
        override def wrap(obj: SimpleFeatureConverter[_]): PooledObject[SimpleFeatureConverter[_]] = new DefaultPooledObject[SimpleFeatureConverter[_]](obj)
      })
  }

  protected def createTypeIfNeeded(ds: DataStore, sft: SimpleFeatureType) = {
    val existingTypes = ds.getTypeNames
    if (!existingTypes.contains(sft.getTypeName)) {
      getLogger.info(s"Creating schema ${sft.getTypeName} ... existing types are ${existingTypes.mkString(", ")}")
      ds.createSchema(sft)
    }
  }

  @OnRemoved
  def cleanup(): Unit = {
    if (dataStore != null) {
      dataStore.dispose()
      dataStore = null
    }

    getLogger.info("Shut down GeoMesaIngest processor " + getIdentifier)
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    import scala.collection.JavaConversions._
    val batchSize: Int = context.getProperty(NifiBatchSize).asInteger()
    val flowFiles = session.get(batchSize)
    getLogger.info(s"Processing ${flowFiles.size()} files in batch")
    val successes = new java.util.ArrayList[FlowFile]()
    if (flowFiles != null && flowFiles.size > 0) {
      val fw: SFW = createFeatureWriter(sft, context)
      try {
        val fn: ProcessFn = ingestMode match {
          case IngestMode.Converter => converterIngester(fw)
          case IngestMode.AvroDataFile => avroIngester(fw)
          case o: String =>
            throw new IllegalStateException(s"Unknown ingest type: $o")
        }
        flowFiles.foreach { f =>
          try {
            getLogger.info(s"Processing file ${fullName(f)}")
            fn(context, session, f)
            successes.add(f)
          } catch {
            case e: Exception =>
              getLogger.error(s"Error: ${e.getMessage}", e)
              session.transfer(f, FailureRelationship)
          }
        }
      } finally {
        fw.close()
      }
      successes.foreach(session.transfer(_, SuccessRelationship))
    }
  }

  // Abstract
  protected def getDataStore(context: ProcessContext): DataStore

  protected def fullName(f: FlowFile): String = f.getAttribute("path") + f.getAttribute("filename")

  protected def getSft(context: ProcessContext): SimpleFeatureType = {
    val sftArg = Option(context.getProperty(SftName).getValue)
      .orElse(Option(context.getProperty(SftSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${SftName.getName} or ${SftSpec.getName} property"))
    val typeName = context.getProperty(FeatureNameOverride).getValue
    SftArgResolver.getArg(SftArgs(sftArg, typeName)) match {
      case Left(e) => throw e
      case Right(sftype) => sftype
    }
  }


  protected def createFeatureWriter(sft: SimpleFeatureType, context: ProcessContext): SFW = {
    dataStore.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
  }

  protected def avroIngester(fw: SFW): ProcessFn =
    (context: ProcessContext, session: ProcessSession, flowFile: FlowFile) => {
      val useProvidedFid =
        Option(context.getProperty(UseProvidedFid).asBoolean()).getOrElse(boolean2Boolean(false))
      val fullFlowFileName = fullName(flowFile)
      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          val reader = new AvroDataFileReader(in)
          try {
            val features = buildConverter(reader.getSft, sft) match {
              case None => reader
              case Some(converter) => reader.map(converter.apply)
            }
            features.foreach { sf =>
              val toWrite = fw.next()
              toWrite.setAttributes(sf.getAttributes)
              toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
              toWrite.getUserData.putAll(sf.getUserData)
              if (useProvidedFid) {
                toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
              }
              try {
                fw.write()
              } catch {
                case e: Exception =>
                  getLogger.warn(s"ERROR writing feature to DataStore '${DataUtilities.encodeFeature(toWrite)}'", e)
              }
            }
          } finally {
            reader.close()
          }
        }
      })
      getLogger.debug(s"Ingested avro file $fullFlowFileName")
    }


  def buildConverter(input: SimpleFeatureType, output: SimpleFeatureType): Option[SimpleFeature => SimpleFeature] = {
    AvroIngest.checkCompatibleSchema(input, output).map { error =>
      if (matchMode == LenientMatch) {
        convert(input, output)
      } else {
        throw error
      }
    }
  }

  protected def converterIngester(fw: SFW): ProcessFn =
    (context: ProcessContext, session: ProcessSession, flowFile: FlowFile) => {
      getLogger.debug("Running converter based ingest")
      val useProvidedFid =
        Option(context.getProperty("UseProvidedFID").asBoolean()).getOrElse(boolean2Boolean(false))
      val converter = converterPool.borrowObject()
      try {
        val fullFlowFileName = fullName(flowFile)
        val ec = converter.createEvaluationContext(Map("inputFilePath" -> fullFlowFileName))
        session.read(flowFile, new InputStreamCallback {
          override def process(in: InputStream): Unit = {
            getLogger.info(s"Converting path $fullFlowFileName")
            converter.process(in, ec).foreach { sf =>
              val toWrite = fw.next()
              toWrite.setAttributes(sf.getAttributes)
              toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
              toWrite.getUserData.putAll(sf.getUserData)
              if (useProvidedFid) {
                toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
              }
              try {
                fw.write()
              } catch {
                case e: Exception =>
                  getLogger.warn(s"ERROR writing feature to DataStore '${DataUtilities.encodeFeature(toWrite)}'", e)
              }
            }
          }
        })
        getLogger.debug(s"Converted and ingested file $fullFlowFileName with ${ec.counter.getSuccess} successes and " +
          s"${ec.counter.getFailure} failures")
      } finally {
        converterPool.returnObject(converter)
      }
    }
}

object AbstractGeoIngestProcessor {

  object Properties {
    val SftName = new PropertyDescriptor.Builder()
      .name("SftName")
      .description("Choose a simple feature type defined by a GeoMesa SFT Provider (preferred)")
      .required(false)
      .allowableValues(SimpleFeatureTypeLoader.listTypeNames.sorted.toArray: _*)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build

    val ConverterName = new PropertyDescriptor.Builder()
      .name("ConverterName")
      .description("Choose an SimpleFeature Converter defined by a GeoMesa SFT Provider (preferred)")
      .required(false)
      .allowableValues(ConverterConfigLoader.listConverterNames.sorted.toArray: _*)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build

    val FeatureNameOverride = new PropertyDescriptor.Builder()
      .name("FeatureNameOverride")
      .description("Override the Simple Feature Type name from the SFT Spec")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build

    val SftSpec = new PropertyDescriptor.Builder()
      .name("SftSpec")
      .description("Manually define a SimpleFeatureType (SFT) config spec")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .addValidator(SimpleFeatureTypeValidator)
      .required(false)
      .build

    val ConverterSpec = new PropertyDescriptor.Builder()
      .name("ConverterSpec")
      .description("Manually define a converter using typesafe config")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .addValidator(ConverterValidator)
      .required(false)
      .build

    val IngestModeProp = new PropertyDescriptor.Builder()
      .name("Mode")
      .description("Ingest mode")
      .required(true)
      .allowableValues(Array[String](IngestMode.Converter, IngestMode.AvroDataFile): _*)
      .defaultValue(IngestMode.Converter)
      .build

    val NifiBatchSize = new PropertyDescriptor.Builder()
      .name("BatchSize")
      .description("Number for Nifi FlowFiles to Batch Together")
      .required(false)
      .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
      .defaultValue("5")
      .build
  }

  object Relationships {
    final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
    final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
  }
}
