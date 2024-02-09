/***********************************************************************
 * Copyright (c) 2015-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor
package service

import org.apache.nifi.annotation.lifecycle.{OnDisabled, OnEnabled}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.controller.{AbstractControllerService, ConfigurationContext}
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
 * Data store controller service
 *
 * @param descriptors data store descriptors
 */
class GeoMesaDataStoreService[T <: DataStoreFactorySpi: ClassTag](descriptors: Seq[PropertyDescriptor])
    extends AbstractControllerService with DataStoreService {

  import scala.collection.JavaConverters._

  protected val params = new java.util.HashMap[String, AnyRef]()
  private val sensitive = descriptors.collect { case d if d.isSensitive => d.getName }.toSet
  private val validateAt = new AtomicLong(-1L)
  private var store: DataStore = _

  override def loadDataStore: DataStore = synchronized {
    if (store == null) {
      store = newDataStore()
    }
    store
  }

  override def newDataStore(): DataStore = tryGetDataStore(params).get

  override def dispose(ds: DataStore): Unit = synchronized {
    if (ds != null && !ds.eq(store)) {
      CloseWithLogging(ds)
    }
  }

  @OnEnabled
  def onEnabled(context: ConfigurationContext): Unit = {
    params.clear()
    getDataStoreParams(context).foreach { case (k, v) => params.put(k, v) }
    logParams("Enabled", params.asScala.toMap)
  }

  @OnDisabled
  def onDisabled(): Unit = synchronized {
    if (store != null) {
      CloseWithLogging(store)
      store = null
    }
  }

  protected def getDataStoreParams(context: PropertyContext): Map[String, _ <: AnyRef] =
    GeoMesaDataStoreService.getDataStoreParams(context, descriptors)

  protected def tryGetDataStore(params: java.util.Map[String, _]): Try[DataStore] =
    GeoMesaDataStoreService.tryGetDataStore[T](params)

  override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String): Unit =
    validateAt.set(-1L)

  override protected def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = descriptors.asJava

  override protected def customValidate(context: ValidationContext): java.util.Collection[ValidationResult] = {
    val now = System.currentTimeMillis()
    if (now < validateAt.get) {
      Collections.emptySet()
    } else {
      val params = getDataStoreParams(context)
      logParams("Validation", params)
      tryGetDataStore(params.asJava) match {
        case Failure(e) =>
          // invalid results will be checked by nifi every 5 seconds
          Collections.singleton(invalid(getClass.getSimpleName, e))
        case Success(ds) =>
          ds.dispose()
          // if valid, only re-check every minute to reduce the overhead of continuously getting a datastore
          validateAt.set(now + 60000L)
          Collections.emptySet()
      }
    }
  }

  private def logParams(phase: String, params: Map[String, _ <: AnyRef]): Unit = {
    def mask(kv: (String, Any)): String = kv match {
      case (k, v) => s"$k -> ${if (sensitive.contains(k)) { "***" } else { v }}"
    }
    getLogger.trace(s"$phase: DataStore parameters: ${params.map(mask(_)).mkString(", ")}")
  }

  override def toString: String = s"${getClass.getSimpleName}[id=$getIdentifier]"
}

object GeoMesaDataStoreService {

  /**
   * Get data store parameter map based on a nifi context
   *
   * @param context context
   * @param props property descriptors corresponding to the data store factory params
   * @return
   */
  def getDataStoreParams(context: PropertyContext, props: Seq[PropertyDescriptor]): Map[String, AnyRef] = {
    val builder = Map.newBuilder[String, AnyRef]
    props.foreach { p =>
      val property = {
        val prop = context.getProperty(p)
        if (p.isExpressionLanguageSupported) { prop.evaluateAttributeExpressions() }  else { prop }
      }
      val value = property.getValue
      if (value != null) {
        builder += p.getName -> value
      }
    }
    builder.result
  }

  def tryGetDataStore[T <: DataStoreFactorySpi: ClassTag](params: java.util.Map[String, _]): Try[DataStore] = {
    Try {
      val factory = implicitly[ClassTag[T]].runtimeClass.newInstance().asInstanceOf[T]
      val store = factory.createDataStore(params.asInstanceOf[java.util.Map[String, java.io.Serializable]])
      if (store == null) {
        throw new IllegalArgumentException("Could not load datastore using provided parameters")
      }
      store
    }
  }
}
