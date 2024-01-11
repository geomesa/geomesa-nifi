/***********************************************************************
 * Copyright (c) 2015-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.gt

import org.apache.nifi.components.PropertyDescriptor
import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.utils.io.CloseWithLogging

import scala.reflect.ClassTag

/**
 * JDBCDataStore has synchronized blocks in the write method that prevent multi-threading.
 * To get around that, we create new stores whenever requested instead of re-using a single store.
 * The JDBCDataStore synchronization is to allow for generated fids from the database.
 * Generally, this shouldn't be an issue since we use provided fids,
 * but running with 1 thread would restore the old behavior.
 *
 * @param descriptors data store descriptors
 */
class JdbcDataStoreService[T <: DataStoreFactorySpi: ClassTag](descriptors: Seq[PropertyDescriptor])
    extends GeoMesaDataStoreService[T](descriptors) {
  override def loadDataStore: DataStore = tryGetDataStore().get
  override def dispose(ds: DataStore): Unit = CloseWithLogging(ds)
}
