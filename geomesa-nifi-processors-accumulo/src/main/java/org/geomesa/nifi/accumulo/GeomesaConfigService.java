/***********************************************************************
 * Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.accumulo;

import org.apache.nifi.controller.ControllerService;
import org.geotools.data.DataStore;


public interface GeomesaConfigService extends ControllerService {
    DataStore getDataStore();
}
