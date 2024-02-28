/***********************************************************************
 * Copyright (c) 2015-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.services;

import org.apache.nifi.controller.ControllerService;
import org.geotools.api.data.DataStore;

public interface DataStoreService extends ControllerService {

    /**
     * Get a {@code DataStore} that may be instance state for this {@code DataStoreService}. Clean up after use by
     * calling {@code DataStoreService.dispose(...)} - do not dispose directly.
     */
    DataStore loadDataStore();

    /**
     * Create a new {@code DataStore} that the caller is responsible for disposing.
     */
    DataStore newDataStore();

    /**
     * Dispose a {@code DataStore}.
     */
    void dispose(DataStore ds);
}
