/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.mixins

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.nifi.components.PropertyValue
import org.apache.nifi.flowfile.FlowFile
import org.geomesa.nifi.datastore.processor.mixins.FeatureWriters.SimpleWriter
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.api.data.{DataStore, FeatureWriter, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.Closeable
import java.time.Duration
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import scala.util.control.NonFatal

/**
 * Abstraction over feature writers
 */
sealed trait FeatureWriters extends Closeable {

  /**
   * Get a feature writer for the given file
   *
   * @param typeName simple feature type name
   * @param file the flow file being operated on
   * @return
   */
  def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T

  /**
   * Invalidate any cached writers
   *
   * @param typeName simple feature type name
   */
  def invalidate(typeName: String): Unit
}

object FeatureWriters {

  import scala.collection.JavaConverters._

  /**
   * Type alias for writing a simple feature
   */
  type SimpleWriter = SimpleFeature => Unit

  /**
   * Gets an appending feature writer
   *
   * @param service data store service
   * @param caching cache timeout in milliseconds, or None for no caching
   * @return
   */
  def appender(service: DataStoreService, caching: Option[Long]): FeatureWriters = {
    caching match {
      case None => new SingletonAppendWriters(service)
      case Some(timeout) => new PooledAppendWriters(service, timeout)
    }
  }

  /**
   * Gets a modifying feature writer
   *
   * @param service data store service
   * @param attribute attribute used to finding features, or None to use feature ID
   * @return
   */
  def modifier(service: DataStoreService, attribute: Option[String]): FeatureWriters =
    new ModifyWriters(service, attribute)

  /**
   * Gets a dynamic feature writer, that can operate in either appending or modifying mode depending on
   * flow file attributes
   *
   * @param service data store service
   * @param mode write mode property
   * @param attribute update attribute property
   * @param caching cache timeout in milliseconds, or None for no caching
   * @return
   */
  def dynamic(service: DataStoreService, mode: PropertyValue, attribute: PropertyValue, caching: Option[Long]): FeatureWriters =
    new DynamicWriters(service, mode, attribute, caching)

  /**
   * Appends features
   *
   * @param writer feature writer
   */
  private class AppendWriter(val writer: FeatureWriter[SimpleFeatureType, SimpleFeature]) extends SimpleWriter {
    override def apply(f: SimpleFeature): Unit = FeatureUtils.write(writer, f)
  }

  /**
   * Modifies existing features
   *
   * @param ds datastore
   * @param typeName feature type name
   * @param attribute attribute to match, or None for feature id
   */
  private class ModifyWriter(ds: DataStore, typeName: String, attribute: Option[String]) extends SimpleWriter with LazyLogging  {

    import FilterHelper.ff

    private var valid = true

    private val toFilter: SimpleFeature => Filter = attribute match {
      case None => f => ff.id(ff.featureId(f.getID))
      case Some(a) => f => ff.equals(ff.property(a), ff.literal(f.getAttribute(a)))
    }

    override def apply(f: SimpleFeature): Unit = {
      val filter = toFilter(f)
      val writer =
        try { ds.getFeatureWriter(typeName, filter, Transaction.AUTO_COMMIT) } catch {
          case NonFatal(e) =>
            // if we can't get the writer, that usually means the datastore has become invalid
            valid = false
            throw e
        }
      try {
        if (writer.hasNext) {
          FeatureUtils.write(writer, f, useProvidedFid = true)
          logger.whenWarnEnabled {
            if (writer.hasNext) {
              logger.warn(s"Filter '${ECQL.toCQL(filter)}' matched multiple records, only updating the first one")
            }
          }
        } else {
          logger.warn(s"Filter '${ECQL.toCQL(filter)}' did not match any records, applying as an insert")
          WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.write(writer, f, useProvidedFid = true)
          }
        }
      } finally {
        CloseWithLogging(writer)
      }
    }

    /**
     * Tracks if the datastore has become invalid
     *
     * @return
     */
    def isValid: Boolean = valid
  }

  /**
   * Each flow file gets a new feature writer, which is closed after use
   *
   * @param service data store service
   */
  private class SingletonAppendWriters(service: DataStoreService) extends FeatureWriters {

    private val queue = new DataStoreQueue(service)

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      val ds = queue.get()
      // if we can't get the writer, that usually means the datastore has become invalid
      val writer =
        try { ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT) } catch {
          case NonFatal(e) =>
            service.dispose(ds)
            throw e
        }
      try {
        fn(new AppendWriter(writer))
      } finally {
        CloseWithLogging(writer)
        queue.put(ds)
      }
    }

    override def invalidate(typeName: String): Unit = {}
    override def close(): Unit = queue.close()
  }

  /**
   * Pooled feature writers, re-used between flow files
   *
   * @param service data store service
   */
  private class PooledAppendWriters(service: DataStoreService, timeout: Long) extends FeatureWriters {

    private type WriterPool = GenericObjectPool[FeatureWriter[SimpleFeatureType, SimpleFeature]]

    private val queue = new DataStoreQueue(service)
    private val pools = new ConcurrentHashMap[String, ConcurrentHashMap[DataStore, WriterPool]]()

    private val poolConfig = {
      val config = new GenericObjectPoolConfig[FeatureWriter[SimpleFeatureType, SimpleFeature]]()
      config.setMaxTotal(-1)
      config.setMaxIdle(-1)
      config.setMinIdle(0)
      config.setMinEvictableIdleDuration(Duration.ofMillis(timeout))
      config.setTimeBetweenEvictionRuns(Duration.ofMillis(math.max(1000, timeout / 5)))
      config.setNumTestsPerEvictionRun(10)
      config
    }


    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      val ds = queue.get()
      val pool = getPool(ds, typeName)
      // if we can't get the writer, that usually means the datastore has become invalid
      val writer =
        try { pool.borrowObject() } catch {
          case NonFatal(e) =>
            pools.get(typeName).remove(ds)
            CloseWithLogging(pool)
            service.dispose(ds)
            throw e
        }

      try {
        fn(new AppendWriter(writer))
      } finally {
        pool.returnObject(writer)
        queue.put(ds)
      }
    }

    override def invalidate(typeName: String): Unit =
      CloseWithLogging(Option(pools.remove(typeName)).toSeq.flatMap(_.values().asScala))

    override def close(): Unit = {
      CloseWithLogging(pools.values().asScala.flatMap(_.values().asScala))
      pools.clear()
      queue.close()
    }

    private def getPool(ds: DataStore, typeName: String): WriterPool =
      pools.computeIfAbsent(typeName, _ => new ConcurrentHashMap[DataStore, WriterPool]())
        .computeIfAbsent(ds, _ => newPool(ds, typeName))

    private def newPool(ds: DataStore, typeName: String): WriterPool = {
      val factory: BasePooledObjectFactory[FeatureWriter[SimpleFeatureType, SimpleFeature]] =
        new BasePooledObjectFactory[FeatureWriter[SimpleFeatureType, SimpleFeature]] {
          override def wrap(obj: FeatureWriter[SimpleFeatureType, SimpleFeature]) = new DefaultPooledObject(obj)
          override def create(): FeatureWriter[SimpleFeatureType, SimpleFeature] =
            ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
          override def destroyObject(p: PooledObject[FeatureWriter[SimpleFeatureType, SimpleFeature]]): Unit = p.getObject.close()
        }
      new GenericObjectPool[FeatureWriter[SimpleFeatureType, SimpleFeature]](factory, poolConfig)
    }
  }

  /**
   * Each record gets an updating writer
   *
   * @param service datastore service
   * @param attribute the unique attribute used to identify the record to update
   */
  private class ModifyWriters(service: DataStoreService, attribute: Option[String]) extends FeatureWriters {

    private val queue = new DataStoreQueue(service)

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      val ds = queue.get()
      val writer = new ModifyWriter(ds, typeName, attribute)
      try {
        fn(writer)
      } finally {
        // if we can't get the writer, that usually means the datastore has become invalid
        // however we can't get the writer here as we don't have the feature attributes, so we track it with a valid flag
        if (writer.isValid) {
          queue.put(ds)
        } else {
          service.dispose(ds)
        }
      }
    }

    override def invalidate(typeName: String): Unit = {}
    override def close(): Unit = queue.close()
  }

  /**
   * Dynamically creates append or modify writers based on flow file attributes
   *
   * @param service data store service
   * @param mode write mode property value
   * @param attribute identifying attribute property value
   * @param caching timeout for caching in millis
   */
  private class DynamicWriters(service: DataStoreService, mode: PropertyValue, attribute: PropertyValue, caching: Option[Long])
      extends FeatureWriters {

    private val appender = FeatureWriters.appender(service, caching)
    private val modifiers = new ConcurrentHashMap[Option[String], FeatureWriters]()

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      mode.evaluateAttributeExpressions(file).getValue match {
        case m if m == null || m.isEmpty || m.equalsIgnoreCase(DataStoreIngestProcessor.AppendMode) =>
          appender.borrow(typeName, file)(fn)

        case m if m.equalsIgnoreCase(DataStoreIngestProcessor.ModifyMode) =>
          appender.invalidate(typeName) // force a write/flush of any pending features
          val attr = Option(attribute.evaluateAttributeExpressions(file).getValue)
          modifiers.computeIfAbsent(attr, _ => FeatureWriters.modifier(service, attr)).borrow(typeName, file)(fn)

        case m =>
          throw new IllegalArgumentException(s"Invalid value for ${DataStoreIngestProcessor.Properties.WriteMode.getName}: $m")
      }
    }

    override def invalidate(typeName: String): Unit = appender.invalidate(typeName)

    override def close(): Unit = {
      CloseWithLogging(appender)
      CloseWithLogging(modifiers.asScala.values)
      modifiers.clear()
    }
  }

  /**
   * Class for tracking/reusing data stores
   *
   * @param service data store service
   */
  private class DataStoreQueue(service: DataStoreService) extends Closeable {

    private val stores = new LinkedBlockingQueue[DataStore]()

    def get(): DataStore = {
      stores.poll() match {
        case null => service.loadDataStore()
        case ds => ds
      }
    }

    def put(ds: DataStore): Unit = stores.put(ds)

    override def close(): Unit = {
      while (!stores.isEmpty) {
        Option(stores.poll()).foreach(service.dispose)
      }
    }
  }
}
