/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.lambda

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.controller.ControllerServiceProxyWrapper
import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.api.data.DataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.lambda.data.{LambdaDataStore, LambdaDataStoreFactory, LambdaDataStoreParams}
import org.locationtech.geomesa.lambda.stream.{OffsetManager, ZookeeperOffsetManager}
import org.locationtech.geomesa.utils.io.CloseWithLogging

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@Tags(Array("geomesa", "geotools", "geo", "kafka"))
@CapabilityDescription("Service for connecting to GeoMesa Lambda stores")
class LambdaDataStoreService
    extends GeoMesaDataStoreService[LambdaDataStoreFactory](LambdaDataStoreService.Properties) with LazyLogging {

  import LambdaDataStoreService.DataStoreService

  override protected def getDataStoreParams(context: PropertyContext): Map[String, _ <: AnyRef] = {
    super.getDataStoreParams(context) ++
        Option(context.getProperty(DataStoreService))
          .map(p => DataStoreService.getName -> p.asControllerService(classOf[DataStoreService]))
          .toMap
  }

  override protected def tryGetDataStore(params: java.util.Map[String, _]): Try[DataStore] = {
    var controller: DataStoreService = null
    var persistence: DataStore = null
    var offsetManager: OffsetManager = null
    try {
      controller = params.get(DataStoreService.getName).asInstanceOf[DataStoreService]
      if (controller == null) {
        throw new IllegalArgumentException("Could not load datastore using provided parameters")
      }
      persistence = controller.newDataStore()
      if (persistence == null) {
        throw new IllegalArgumentException(
          s"Could not load datastore from controller service ${controller.getClass.getName}")
      }
      val catalog = {
        // unwrap the proxied datastore service returned by nifi
        // GeoMesaDataStore is not an interface, so the proxy won't expose its methods directly
        val unproxied =
          Some(persistence).collect { case p: ControllerServiceProxyWrapper[_] => p.getWrapped }.collect { case d: DataStore => d }
        val ds = unproxied.getOrElse {
          logger.warn(s"Failed to unwrap controller service proxy of type: ${persistence.getClass.getName}")
          persistence
        }
        ds match {
          case gm: GeoMesaDataStore[_] => gm.config.catalog
          case _ => "nifi"
        }
      }
      val config = LambdaDataStoreParams.parse(params, catalog)
      offsetManager = new ZookeeperOffsetManager(config.zookeepers, config.zkNamespace)
      Success(new LambdaDataStore(persistence, offsetManager, config))
    } catch {
      case NonFatal(e) =>
        if (offsetManager != null) {
          CloseWithLogging(offsetManager)
        }
        if (controller != null) {
          controller.dispose(persistence)
        }
        Failure(e)
    }
  }
}

object LambdaDataStoreService extends PropertyDescriptorUtils {

  import org.locationtech.geomesa.lambda.data.LambdaDataStoreParams._

  val DataStoreService: PropertyDescriptor =
    new PropertyDescriptor.Builder()
        .name("DataStoreService")
        .displayName("DataStore Service")
        .required(true)
        .description("The DataStore to use for long-term persistence")
        .identifiesControllerService(classOf[DataStoreService])
        .build()

  // note: LambdaDataStoreFactory.ParameterInfo has Accumulo connection params, but we allow any store
  val Properties: Seq[PropertyDescriptor] =
    Seq(DataStoreService) ++ Seq(
      BrokersParam,
      ZookeepersParam,
      ExpiryParam,
      PersistParam,
      BatchSizeParam,
      ProducerOptsParam,
      ConsumerOptsParam,
      ConsumersParam,
      PartitionsParam,
      QueryThreadsParam,
      QueryTimeoutParam,
      GenerateStatsParam,
      AuditQueriesParam,
      LooseBBoxParam,
      AuthsParam,
      ForceEmptyAuthsParam
    ).map(createPropertyDescriptor)
}
