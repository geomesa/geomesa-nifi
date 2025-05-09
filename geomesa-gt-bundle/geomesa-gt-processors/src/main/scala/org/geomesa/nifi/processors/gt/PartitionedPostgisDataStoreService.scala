/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.gt

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.locationtech.geomesa.gt.partition.postgis.PartitionedPostgisDataStoreFactory

@Tags(Array("geomesa", "geotools", "geo", "postgis"))
@CapabilityDescription("Service for connecting to GeoMesa partitioned PostGIS stores")
class PartitionedPostgisDataStoreService
    extends JdbcDataStoreService[PartitionedPostgisDataStoreFactory](PartitionedPostgisDataStoreService.Parameters)

object PartitionedPostgisDataStoreService extends JdbcPropertyDescriptorUtils {
  private val Parameters = createPropertyDescriptors(new PartitionedPostgisDataStoreFactory().getParametersInfo.toSeq)
}

