/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.gt

import org.geomesa.nifi.datastore.services.DataStoreService
import org.geotools.data.DataStore

trait JdbcDataStoreService extends DataStoreService {

  import scala.collection.JavaConverters._

  override def loadDataStores(count: Int): java.util.List[DataStore] = {
    require(count > 0, s"Count must be > 0: $count")
    // JDBCDataStore has synchronized blocks in the write method that prevent multi-threading,
    // the synchronization is to allow for generated fids from the database.
    // generally, this shouldn't be an issue since we use provided fids,
    // but running with 1 thread would restore the old behavior
    Seq.fill(count)(loadDataStore()).asJava
  }
}
