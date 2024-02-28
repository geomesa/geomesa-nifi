/***********************************************************************
 * Copyright (c) 2015-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.gt

import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnDisabled, OnEnabled}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.controller.{AbstractControllerService, ConfigurationContext, ControllerServiceInitializationContext}
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geomesa.nifi.datastore.processor.{ExtraClasspaths, invalid}
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geomesa.nifi.processors.gt.GeoToolsDataStoreService.StoreManager
import org.geotools.api.data.{DataStore, DataStoreFactorySpi, DataStoreFinder}
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.util.{Failure, Success, Try}

// instance classloading resolves some issues with DataStoreFactories finding their dependencies
@RequiresInstanceClassLoading
@Tags(Array("OGC", "geo", "simple feature", "geotools", "geomesa"))
@CapabilityDescription("Manages a connection to an arbitrary GeoTools data store")
class GeoToolsDataStoreService extends AbstractControllerService with DataStoreService {

  import scala.collection.JavaConverters._

  private val descriptors = new java.util.ArrayList[PropertyDescriptor]()
  private val stores = Collections.newSetFromMap(new ConcurrentHashMap[DataStore, java.lang.Boolean]())
  private val valid = new AtomicReference[java.util.Collection[ValidationResult]](null)

  private val manager = new StoreManager()

  override def init(config: ControllerServiceInitializationContext): Unit = {
    descriptors.clear()
    manager.reload()
    descriptors.add(ExtraClasspaths)
    manager.properties.foreach(descriptors.add)
  }

  override final def loadDataStore: DataStore = {
    val store = newDataStore()
    if (store == null) {
      throw new RuntimeException("Could not load data store using configured parameters")
    }
    stores.add(store)
    store
  }

  override final def newDataStore(): DataStore = manager.tryLoadStore().get

  override final def dispose(ds: DataStore): Unit = {
    stores.remove(ds)
    CloseWithLogging(ds)
  }

  @OnEnabled
  final def onEnabled(context: ConfigurationContext): Unit = {
    manager.configure(context)
  }

  @OnDisabled
  final def onDisabled(): Unit = {
    stores.asScala.foreach(CloseWithLogging(_))
    stores.clear()
  }

  override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String): Unit =
    valid.set(null)

  override protected def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] =
    Collections.unmodifiableList(descriptors)

  override protected def customValidate(context: ValidationContext): java.util.Collection[ValidationResult] = {
    Option(valid.get).getOrElse {
      manager.configure(context)
      val result = manager.tryLoadStore() match {
        case Success(ds)   => ds.dispose(); Collections.emptySet[ValidationResult]()
        case Failure(e)    => Collections.singleton(invalid(getClass.getSimpleName, e))
      }
      valid.set(result)
      result
    }
  }
}

object GeoToolsDataStoreService extends PropertyDescriptorUtils {

  import scala.collection.JavaConverters._

  private class StoreManager {

    private var factories: Seq[DataStoreFactorySpi] = _
    private var storeName: PropertyDescriptor = _
    private var props: Seq[PropertyDescriptor] = _
    private var factory: Try[DataStoreFactorySpi] = _
    private var params: java.util.Map[String, AnyRef] = Collections.emptyMap()

    def reload(): Unit = {
      DataStoreFinder.scanForPlugins() // ensure stores on the current classpath are loaded
      factories = DataStoreFinder.getAvailableDataStores.asScala.toList.sortBy(_.getDisplayName)
      val missing = DataStoreFinder.getAllDataStores.asScala.toList.diff(factories)
      if (missing.nonEmpty) {
        logger.warn(s"Found unavailable data store implementations: ${missing.map(_.getClass.getName).mkString(", ")}")
      }
      storeName =
        new PropertyDescriptor.Builder()
            .name("DataStoreName")
            .required(true)
            .description("The GeoTools data store type to use")
            .allowableValues(factories.map(_.getDisplayName): _*)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build()
      props = factories.flatMap { factory =>
        factory.getParametersInfo.map { param =>
          new PropertyDescriptor.Builder()
              .fromPropertyDescriptor(createPropertyDescriptor(param))
              .dependsOn(storeName, factory.getDisplayName)
              .build()
        }
      }
    }

    def configure(context: PropertyContext): Unit = {
      val name = context.getProperty(storeName).getValue
      factory = factories.find(_.getDisplayName == name) match {
        case None => Failure(new IllegalArgumentException(s"Could not load factory with name '$name'"))
        case Some(f) => Success(f)
      }
      params = GeoMesaDataStoreService.getDataStoreParams(context, props).asJava
    }

    def properties: Seq[PropertyDescriptor] = Seq(storeName) ++ props

    def tryLoadStore(): Try[DataStore] = factory.flatMap(f => Try(f.createDataStore(params)))
  }
}