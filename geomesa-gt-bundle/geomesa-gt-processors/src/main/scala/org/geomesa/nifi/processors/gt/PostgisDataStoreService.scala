/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.gt

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.geotools.data.postgis.PostgisNGDataStoreFactory

@Tags(Array("geomesa", "geotools", "geo", "postgis"))
@CapabilityDescription("Service for connecting to PostGIS stores")
class PostgisDataStoreService
    extends JdbcDataStoreService[PostgisNGDataStoreFactory](PostgisDataStoreService.Parameters)

object PostgisDataStoreService extends JdbcPropertyDescriptorUtils {
  private val Parameters = createPropertyDescriptors(new PostgisNGDataStoreFactory().getParametersInfo.toSeq)
}
