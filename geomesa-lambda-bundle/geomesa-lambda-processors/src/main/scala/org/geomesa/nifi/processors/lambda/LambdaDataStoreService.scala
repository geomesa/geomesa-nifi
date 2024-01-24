/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.lambda

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.data.DataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.data.{LambdaDataStore, LambdaDataStoreFactory, LambdaDataStoreParams}
import org.locationtech.geomesa.utils.io.CloseWithLogging

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@Tags(Array("geomesa", "geotools", "geo", "kafka"))
@CapabilityDescription("Service for connecting to GeoMesa Lambda stores")
class LambdaDataStoreService
    extends GeoMesaDataStoreService[LambdaDataStoreFactory](LambdaDataStoreService.Properties) {

  import LambdaDataStoreService.DataStoreService

  override protected def getDataStoreParams(context: PropertyContext): Map[String, _ <: AnyRef] = {
    super.getDataStoreParams(context) ++
        Option(context.getProperty(DataStoreService))
          .map(p => DataStoreService.getName -> p.asControllerService(classOf[DataStoreService]))
          .toMap
  }

  override protected def tryGetDataStore(params: java.util.Map[String, _]): Try[DataStore] = {
    var persistence: DataStore = null
    var config: LambdaConfig = null
    try {
      val controller = params.get(DataStoreService.getName).asInstanceOf[DataStoreService]
      if (controller == null) {
        throw new IllegalArgumentException("Could not load datastore using provided parameters")
      }
      persistence = controller.loadDataStore()
      if (persistence == null) {
        throw new IllegalArgumentException(
          s"Could not load datastore from controller service ${controller.getClass.getName}")
      }
      val catalog = persistence match {
        case gm: GeoMesaDataStore[_] => gm.config.catalog
        case _ => "nifi"
      }
      config = LambdaDataStoreParams.parse(params, catalog)
      Success(new LambdaDataStore(persistence, config))
    } catch {
      case NonFatal(e) =>
        if (config != null) {
          CloseWithLogging(config.offsetManager)
        }
        if (persistence != null) {
          CloseWithLogging(persistence)
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
