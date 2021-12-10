/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.services;

import org.apache.nifi.controller.ControllerService;
import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface DataStoreService extends ControllerService {

    Logger logger = LoggerFactory.getLogger(DataStoreService.class);

    java.util.Map<String, ?> getDataStoreParams();

    DataStore loadDataStore();

    default void dispose(DataStore ds) {
        // default implementation to avoid API breakage
        // TODO remove in next major release
        logger.warn("Not implemented: dispose in " + getClass().getName());
    }
}
