/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.mixins

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.ProcessContext
import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.data.DataStore
import org.locationtech.geomesa.utils.io.CloseWithLogging

/**
 * Abstract processor that uses a data store
 */
trait DataStoreProcessor extends BaseProcessor {

  import DataStoreProcessor.Properties.DataStoreService

  protected def loadDataStore(context: ProcessContext): DataStore =
    context.getProperty(DataStoreService).asControllerService(classOf[DataStoreService]).loadDataStore()

  protected def disposeDataStore(ds: DataStore, context: Option[ProcessContext]): Unit = {
    context match {
      case Some(c) => c.getProperty(DataStoreService).asControllerService(classOf[DataStoreService]).dispose(ds)
      case None => CloseWithLogging(ds)
    }
  }

  override protected def getTertiaryProperties: Seq[PropertyDescriptor] =
    Seq(DataStoreService) ++ super.getTertiaryProperties
}

object DataStoreProcessor {
  object Properties {
    val DataStoreService: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("DataStoreService")
          .displayName("DataStore Service")
          .required(true)
          .description("The DataStore to use")
          .identifiesControllerService(classOf[DataStoreService])
          .build()
  }
}

