/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
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

  private val params = new java.util.HashMap[String, AnyRef]()
  private val sensitive = descriptors.collect { case d if d.isSensitive => d.getName }.toSet
  private val stores = Collections.newSetFromMap(new ConcurrentHashMap[DataStore, java.lang.Boolean]())
  private val valid = new AtomicReference[java.util.Collection[ValidationResult]](null)

  override final def loadDataStore: DataStore = {
    val store = tryGetDataStore(params).get
    stores.add(store)
    store
  }

  override final def dispose(ds: DataStore): Unit = {
    stores.remove(ds)
    CloseWithLogging(ds)
  }

  @OnEnabled
  final def onEnabled(context: ConfigurationContext): Unit = {
    params.clear()
    getDataStoreParams(context).foreach { case (k, v) => params.put(k, v) }
    logParams("Enabled", params.asScala.toMap)
  }

  @OnDisabled
  final def onDisabled(): Unit = {
    stores.asScala.foreach(CloseWithLogging(_))
    stores.clear()
  }

  protected def getDataStoreParams(context: PropertyContext): Map[String, _ <: AnyRef] =
    GeoMesaDataStoreService.getDataStoreParams(context, descriptors)

  protected def tryGetDataStore(params: java.util.Map[String, AnyRef]): Try[DataStore] =
    GeoMesaDataStoreService.tryGetDataStore[T](params)

  override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String): Unit =
    valid.set(null)

  override protected def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = descriptors.asJava

  override protected def customValidate(context: ValidationContext): java.util.Collection[ValidationResult] = {
    Option(valid.get).getOrElse {
      val params = getDataStoreParams(context)
      logParams("Validation", params)
      val result = tryGetDataStore(params.asJava) match {
        case Success(ds) => ds.dispose(); Collections.emptySet[ValidationResult]()
        case Failure(e)  => Collections.singleton(invalid(getClass.getSimpleName, e))
      }
      valid.set(result)
      result
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
