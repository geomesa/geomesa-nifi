/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.mixins

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.processor.ProcessContext
import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geotools.data.{DataStore, DataStoreFinder}

/**
 * Abstract processor that uses a data store
 *
 * @param dataStoreProperties properties exposed through NiFi used to load the data store
 */
abstract class AbstractDataStoreProcessor(dataStoreProperties: Seq[PropertyDescriptor]) extends DataStoreProcessor {

  import scala.collection.JavaConverters._

  override protected def loadDataStore(context: ProcessContext): DataStore = {
    val props = getDataStoreParams(context)
    lazy val safeToLog = {
      val sensitive = context.getProperties.keySet().asScala.collect { case p if p.isSensitive => p.getName }
      props.map { case (k, v) => s"$k -> ${if (sensitive.contains(k)) { "***" } else { v }}" }
    }
    logger.trace(s"DataStore properties: ${safeToLog.mkString(", ")}")
    val ds = DataStoreFinder.getDataStore(props.asJava)
    require(ds != null, "Could not load datastore using provided parameters")
    ds
  }

  /**
   * Get params for looking up the data store, based on the current processor configuration
   *
   * @param context context
   * @return
   */
  protected def getDataStoreParams(context: ProcessContext): Map[String, _] =
    GeoMesaDataStoreService.getDataStoreParams(context, dataStoreProperties)

  override protected def getTertiaryProperties: Seq[PropertyDescriptor] =
    (super.getTertiaryProperties ++ dataStoreProperties).filterNot(_ == DataStoreProcessor.Properties.DataStoreService)
}
