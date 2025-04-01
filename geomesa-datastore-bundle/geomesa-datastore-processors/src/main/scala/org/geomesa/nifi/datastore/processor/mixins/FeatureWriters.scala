/***********************************************************************
 * Copyright (c) 2015-2024 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.mixins

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.nifi.flowfile.FlowFile
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.api.data.{DataStore, FeatureWriter, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.io.Closeable
import java.util.concurrent.LinkedBlockingQueue
import scala.util.control.NonFatal

/**
 * Abstraction over feature writers
 */
trait FeatureWriters extends Closeable {

  /**
   * Get a feature writer for the given file
   *
   * @param typeName simple feature type name
   * @param file the flow file being operated on
   * @return
   */
  def borrow[T](typeName: String, file: FlowFile)(fn: FeatureWriters.SimpleWriter => T): T

  /**
   * Invalidate any cached writers
   *
   * @param typeName simple feature type name
   */
  def invalidate(typeName: String): Unit
}

object FeatureWriters {

  import scala.collection.JavaConverters._

  sealed trait WriteMode

  object WriteMode {
    case object Append extends WriteMode
    case class Modify(attribute: Option[String]) extends WriteMode
  }

  /**
   * Create feature writers
   *
   * @param service data store service to use
   * @param mode write mode
   * @param caching cache timeout in milliseconds, or None for no caching
   * @return
   */
  @deprecated("Use appender or modifier")
  def apply(service: DataStoreService, mode: WriteMode, caching: Option[Long]): FeatureWriters = {
    mode match {
      case WriteMode.Append => appender(service, caching)
      case WriteMode.Modify(attribute) => modifier(service, attribute)
    }
  }

  /**
   * Gets an appending feature writer
   *
   * @param service data store service
   * @param caching cache timeout in milliseconds, or None for no caching
   * @return
   */
  def appender(service: DataStoreService, caching: Option[Long]): FeatureWriters = {
    caching match {
      case None => new SingletonWriters(service)
      case Some(timeout) => new PooledWriters(service, timeout)
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
   * Writes for simple features
   */
  trait SimpleWriter extends (SimpleFeature => Unit) with Closeable

  /**
   * Appends features, closes writer upon completion
   *
   * @param writer feature writer
   */
  private class AppendWriter(writer: FeatureWriter[SimpleFeatureType, SimpleFeature]) extends SimpleWriter {
    override def apply(f: SimpleFeature): Unit = FeatureUtils.write(writer, f)
    override def close(): Unit = CloseWithLogging(writer)
  }

  /**
   * Modifies existing features
   *
   * @param ds datastore
   * @param typeName feature type name
   * @param attribute attribute to match, or None for feature id
   */
  private class ModifyWriter(ds: DataStore, typeName: String, attribute: Option[String])
      extends SimpleWriter with LazyLogging  {

    import FilterHelper.ff

    private[FeatureWriters] var invalid = false

    override def apply(f: SimpleFeature): Unit = {
      val filter = attribute match {
        case None    => ff.id(ff.featureId(f.getID))
        case Some(a) => ff.equals(ff.property(a), ff.literal(f.getAttribute(a)))
      }
      val writer = try {
        ds.getFeatureWriter(typeName, filter, Transaction.AUTO_COMMIT)
      } catch {
        case NonFatal(e) =>
          invalid = true
          throw e
      }
      try {
        if (writer.hasNext) {
          FeatureUtils.write(writer, f, useProvidedFid = true)
          if (writer.hasNext) {
            logger.warn(s"Filter '${ECQL.toCQL(filter)}' matched multiple records, only updating the first one")
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

    override def close(): Unit = {}
  }

  /**
   * Each flow file gets a new feature writer, which is closed after use
   *
   * @param service data store service
   */
  private class SingletonWriters(service: DataStoreService) extends FeatureWriters {

    private val stores = new LinkedBlockingQueue[DataStore]()

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      val ds = stores.poll() match {
        case null => service.loadDataStore()
        case ds => ds
      }
      // if we can't get the writer, that usually means the datastore has become invalid
      val writer = try {
        new AppendWriter(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT))
      } catch {
        case NonFatal(e) =>
          service.dispose(ds)
          throw e
      }
      try {
        fn(writer)
      } finally {
        CloseWithLogging(writer)
        stores.put(ds)
      }
    }

    override def invalidate(typeName: String): Unit = {}
    override def close(): Unit = {}
  }

  /**
   * Pooled feature writers, re-used between flow files
   *
   * @param service data store service
   */
  private class PooledWriters(service: DataStoreService, timeout: Long) extends FeatureWriters {

    private val stores = new LinkedBlockingQueue[(DataStore, LoadingCache[String, AppendWriterPool])]()

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      val (ds, cache) = stores.poll() match {
        case null =>
          val ds = service.loadDataStore()
          val cache = Caffeine.newBuilder().build[String, AppendWriterPool](
            new CacheLoader[String, AppendWriterPool] {
              override def load(key: String): AppendWriterPool = new AppendWriterPool(ds, key, timeout)
            })
          (ds, cache)

        case dsAndCache => dsAndCache
      }
      val pool = cache.get(typeName)
      // if we can't get the writer, that usually means the datastore has become invalid
      val writer = try {
        pool.borrow()
      } catch {
        case NonFatal(e) =>
          cache.invalidate(typeName)
          CloseWithLogging(pool)
          service.dispose(ds)
          throw e
      }

      try {
        fn(writer)
      } finally {
        pool.returnIt(writer)
        stores.put(ds -> cache)
      }
    }

    override def invalidate(typeName: String): Unit = {
      stores.iterator().asScala.foreach { case (_, cache) =>
        val cur = cache.getIfPresent(typeName)
        if (cur != null) {
          cache.invalidate(typeName)
          CloseWithLogging(cur)
        }

      }
    }

    override def close(): Unit = {
      stores.iterator().asScala.foreach { case (ds, cache) =>
        CloseWithLogging(cache.asMap().asScala.values)
        service.dispose(ds)
      }
    }
  }

  /**
   * Each record gets an updating writer
   *
   * @param service datastore service
   * @param attribute the unique attribute used to identify the record to update
   */
  private class ModifyWriters(service: DataStoreService, attribute: Option[String])
      extends FeatureWriters {

    private val stores = new LinkedBlockingQueue[DataStore]()

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      val ds = stores.poll() match {
        case null => service.loadDataStore()
        case ds => ds
      }
      val writer = new ModifyWriter(ds, typeName, attribute)
      try {
        fn(writer)
      } finally {
        CloseWithLogging(writer)
        if (writer.invalid) {
          service.dispose(ds)
        } else {
          stores.put(ds)
        }
      }
    }

    override def invalidate(typeName: String): Unit = {}
    override def close(): Unit = stores.iterator().asScala.foreach(service.dispose)
  }

  /**
   * Pool for append writers
   *
   * @param ds data store
   * @param typeName feature type
   * @param timeout writer timeout/refresh
   */
  private class AppendWriterPool(ds: DataStore, typeName: String, timeout: Long) extends Closeable {

    private val factory: BasePooledObjectFactory[AppendWriter] = new BasePooledObjectFactory[AppendWriter] {
      override def wrap(obj: AppendWriter) = new DefaultPooledObject[AppendWriter](obj)
      override def create(): AppendWriter = new AppendWriter(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT))
      override def destroyObject(p: PooledObject[AppendWriter]): Unit = p.getObject.close()
    }

    private val config = {
      val config = new GenericObjectPoolConfig[AppendWriter]()
      config.setMaxTotal(-1)
      config.setMaxIdle(-1)
      config.setMinIdle(0)
      config.setMinEvictableIdleTimeMillis(timeout)
      config.setTimeBetweenEvictionRunsMillis(math.max(1000, timeout / 5))
      config.setNumTestsPerEvictionRun(10)
      config
    }

    private val pool = new GenericObjectPool[AppendWriter](factory, config)

    def borrow(): AppendWriter = pool.borrowObject()
    def returnIt(writer: AppendWriter): Unit = pool.returnObject(writer)

    override def close(): Unit = pool.close()
  }
}
