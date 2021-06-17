/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.accumulo

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory

@Tags(Array("geomesa", "geotools", "geo", "accumulo"))
@CapabilityDescription("Service for connecting to GeoMesa Accumulo stores")
class AccumuloDataStoreService
    extends GeoMesaDataStoreService[AccumuloDataStoreFactory](AccumuloDataStoreService.Properties)

object AccumuloDataStoreService extends PropertyDescriptorUtils {
  val Properties: Seq[PropertyDescriptor] = createPropertyDescriptors(AccumuloDataStoreFactory)
}
