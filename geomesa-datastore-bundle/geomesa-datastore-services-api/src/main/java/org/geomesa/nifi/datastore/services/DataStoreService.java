/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.services;

import org.apache.nifi.controller.ControllerService;
import org.geotools.data.DataStore;

import java.util.ArrayList;
import java.util.List;

public interface DataStoreService extends ControllerService {

    /**
     * Load multiple data stores at once for use in multiple threads. The default
     * implementation assumes data stores are thread safe and just returns a single instance
     * multiple times.
     *
     * @param count number of stores to load, must be positive
     * @return list of data stores
     */
    default List<DataStore> loadDataStores(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("Count must be > 0: " + count);
        }
        DataStore store = loadDataStore();
        List<DataStore> list = new ArrayList<>(count);
        for (int i = 0; i < count; i ++) {
            list.add(store);
        }
        return list;
    }

    DataStore loadDataStore();

    void dispose(DataStore ds);
}
