/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.services;

import org.apache.nifi.controller.ControllerService;

import java.io.Serializable;
import java.util.Map;

public interface DataStoreConfigService extends ControllerService {
    Map<String, Serializable> getDataStoreParameters();
}
