/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor
package mixins

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnShutdown, OnStopped}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.controller.ControllerService
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.ProcessContext
import org.geomesa.nifi.datastore.processor.mixins.DataStoreProcessor.DataStoreProcessorConfig
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.io.IsCloseableImplicits.IterableIsCloseable

/**
 * Abstract processor that uses a data store
 *
 * @param dataStoreProperties properties exposed through NiFi used to load the data store
 */
abstract class DataStoreProcessor(dataStoreProperties: Seq[PropertyDescriptor]) extends BaseProcessor {

  import scala.collection.JavaConverters._

  private val dataStoreCache = {
    val loader = new CacheLoader[Map[String, String], DataStore]() {
      override def load(key: Map[String, String]): DataStore = {
        logger.info("Initializing datastore")
        val ds = DataStoreFinder.getDataStore(key.asJava)
        require(ds != null, "Could not load datastore using provided parameters")
        logger.info(s"Initialized datastore ${ds.getClass.getSimpleName}")
        ds
      }
    }
    Caffeine.newBuilder()
        .expireAfterAccess(1L, TimeUnit.HOURS)
        .removalListener(new CloseableRemovalListener[DataStore]())
        .build(loader)
  }

  @OnRemoved
  @OnStopped
  @OnShutdown
  final def onStop(): Unit = {
    logger.info("Processor shutting down")
    val start = System.currentTimeMillis()
    cleanup()
    logger.info(s"Shut down in ${System.currentTimeMillis() - start}ms")
  }

  protected def cleanup(): Unit = {
    CloseWithLogging(dataStoreCache.asMap().asScala.values)(new IterableIsCloseable[DataStore]())
    dataStoreCache.invalidateAll()
  }

  protected def getCachedDataStore(params: Map[String, String]): DataStore = dataStoreCache.get(params)

  /**
   * Get params for looking up the data store, based on the current processor configuration
   *
   * @param context context
   * @return
   */
  protected def getDataStoreParams(context: ProcessContext): Map[String, String] =
    evaluateDescriptors(dataStoreProperties, context).map { case (d, v) => d.getName -> v }

  override protected def getTertiaryProperties: Seq[PropertyDescriptor] =
    super.getTertiaryProperties ++ dataStoreProperties

  protected def loadProperties(context: ProcessContext, file: FlowFile): DataStoreProcessorConfig = {
    val dsParams = getDataStoreParams(context)

    lazy val safeToLog = {
      val sensitive = context.getProperties.keySet().asScala.collect { case p if p.isSensitive => p.getName }
      dsParams.map { case (k, v) => s"$k -> ${if (sensitive.contains(k)) { "***" } else { v }}" }
    }
    logger.trace(s"DataStore properties: ${safeToLog.mkString(", ")}")

    val properties = Map.newBuilder[PropertyDescriptor, String]
    val services = Map.newBuilder[PropertyDescriptor, ControllerService]

    getSupportedPropertyDescriptors.asScala.foreach { descriptor =>
      if (!dsParams.contains(descriptor.getName)) {
        if (descriptor.getControllerServiceDefinition != null) {
          Option(context.getProperty(descriptor).asControllerService()).foreach(services += descriptor -> _)
        } else {
          evaluateDescriptor(descriptor, context, file).foreach(properties += descriptor -> _)
        }
      }
    }

    DataStoreProcessorConfig(dsParams, properties.result, services.result)
  }
}

object DataStoreProcessor {

  /**
   * Get data store parameter map based on a nifi context
   *
   * @param context context
   * @param props property descriptors corresponding to the data store factory params
   * @return
   */
  def getDataStoreParams(context: PropertyContext, props: Seq[PropertyDescriptor]): Map[String, String] = {
    val builder = Map.newBuilder[String, String]
    props.foreach(p => evaluateDescriptor(p, context).foreach(builder += p.getName -> _))
    builder.result
  }

  case class DataStoreProcessorConfig(
      dsParams: Map[String, String],
      properties: Map[PropertyDescriptor, String],
      services: Map[PropertyDescriptor, ControllerService]
    )
}
