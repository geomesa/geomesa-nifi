/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.mixins

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.nifi.flowfile.FlowFile
import org.geotools.data.{DataStore, FeatureWriter, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.CloseablePool.CommonsPoolPool
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.Closeable
import java.util.Collections
import java.util.concurrent.LinkedBlockingQueue

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
   * @param stores data stores to use, one per thread
   * @param mode write mode
   * @param caching cache timeout in milliseconds, or None for no caching
   * @return
   */
  def apply(stores: Seq[DataStore], mode: WriteMode, caching: Option[Long]): FeatureWriters = {
    val queue = new LinkedBlockingQueue[DataStore](stores.asJava)
    mode match {
      case WriteMode.Append => caching.map(new PooledWriters(queue, _)).getOrElse(new SingletonWriters(queue))
      case WriteMode.Modify(attribute) => new ModifyWriters(queue, attribute, caching)
    }
  }

  /**
   * Writes for simple features
   */
  trait SimpleWriter extends (SimpleFeature => Unit) with Closeable

  /**
   * Appends features, closes writer upon completion
   *
   * @param writer feature writer
   */
  class AppendWriter(writer: FeatureWriter[SimpleFeatureType, SimpleFeature]) extends SimpleWriter {
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
  class ModifyWriter(ds: DataStore, typeName: String, attribute: Option[String], appender: SimpleWriter)
      extends SimpleWriter with LazyLogging  {

    import FilterHelper.ff

    override def apply(f: SimpleFeature): Unit = {
      val filter = attribute match {
        case None    => ff.id(ff.featureId(f.getID))
        case Some(a) => ff.equals(ff.property(a), ff.literal(f.getAttribute(a)))
      }
      WithClose(ds.getFeatureWriter(typeName, filter, Transaction.AUTO_COMMIT)) { writer =>
        if (writer.hasNext) {
          FeatureUtils.write(writer, f, useProvidedFid = true)
          if (writer.hasNext) {
            logger.warn(s"Filter '${ECQL.toCQL(filter)}' matched multiple records, only updating the first one")
          }
        } else {
          appender.apply(f)
        }
      }
    }

    override def close(): Unit = {}
  }

  /**
   * Each flow file gets a new feature writer, which is closed after use
   *
   * @param stores data stores
   */
  class SingletonWriters(stores: LinkedBlockingQueue[DataStore]) extends FeatureWriters {

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      val ds = stores.poll()
      try {
        WithClose(new AppendWriter(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)))(fn)
      } finally {
        stores.put(ds)
      }
    }

    override def invalidate(typeName: String): Unit = {}
    override def close(): Unit = {}
  }

  /**
   * Pooled feature writers, re-used between flow files
   *
   * @param stores data stores
   */
  class PooledWriters(stores: LinkedBlockingQueue[DataStore], timeout: Long) extends FeatureWriters {

    private val caches: Map[DataStore, LoadingCache[String, CloseablePool[SimpleWriter]]] = {
      val seq = stores.asScala.map { ds =>
        ds -> Caffeine.newBuilder().build[String, CloseablePool[SimpleWriter]](
          new CacheLoader[String, CloseablePool[SimpleWriter]] {
            override def load(key: String): CloseablePool[SimpleWriter] = {
              val config = new GenericObjectPoolConfig[SimpleWriter]()
              config.setMaxTotal(-1)
              config.setMaxIdle(-1)
              config.setMinIdle(0)
              config.setMinEvictableIdleTimeMillis(timeout)
              config.setTimeBetweenEvictionRunsMillis(math.max(1000, timeout / 5))
              config.setNumTestsPerEvictionRun(10)

              new CommonsPoolPool(new AppendWriter(ds.getFeatureWriterAppend(key, Transaction.AUTO_COMMIT)), config)
            }
          }
        )
      }
      seq.toMap
    }

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      val ds = stores.poll()
      try {
        caches(ds).get(typeName).borrow(fn)
      } finally {
        stores.put(ds)
      }
    }

    override def invalidate(typeName: String): Unit = {
      caches.foreach { case (_, cache) =>
        val cur = cache.get(typeName)
        cache.invalidate(typeName)
        CloseWithLogging(cur)
      }
    }

    override def close(): Unit = caches.foreach { case (_, c) => CloseWithLogging(c.asMap().asScala.values) }
  }

  /**
   * Each record gets an updating writer
   *
   * @param stores datastores
   * @param attribute the unique attribute used to identify the record to update
   */
  class ModifyWriters(stores: LinkedBlockingQueue[DataStore], attribute: Option[String], caching: Option[Long])
      extends FeatureWriters {

    private val appenders: Map[DataStore, FeatureWriters] = {
      val seq = stores.asScala.map { ds =>
        val queue = new LinkedBlockingQueue(Collections.singleton(ds))
        val appender = caching match {
          case None => new SingletonWriters(queue)
          case Some(timeout) => new PooledWriters(queue, timeout)
        }
        ds -> appender
      }
      seq.toMap
    }

    override def borrow[T](typeName: String, file: FlowFile)(fn: SimpleWriter => T): T = {
      val ds = stores.poll()
      try {
        appenders(ds).borrow(typeName, file) { appender =>
          WithClose(new ModifyWriter(ds, typeName, attribute, appender))(fn)
        }
      } finally {
        stores.put(ds)
      }
    }

    override def invalidate(typeName: String): Unit = appenders.values.foreach(_.invalidate(typeName))
    override def close(): Unit = CloseWithLogging(appenders.values)
  }
}
