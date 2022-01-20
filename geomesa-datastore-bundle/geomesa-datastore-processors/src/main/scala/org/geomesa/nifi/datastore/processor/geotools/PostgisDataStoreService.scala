/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.geotools

import org.geomesa.nifi.datastore.processor.service.GeoMesaDataStoreService
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geotools.data.postgis.PostgisNGDataStoreFactory

class PostgisDataStoreService
    extends GeoMesaDataStoreService[PostgisNGDataStoreFactory](PostgisDataStoreService.Parameters)

object PostgisDataStoreService extends PropertyDescriptorUtils {
  private val Parameters = new PostgisNGDataStoreFactory().getParametersInfo.toSeq.map(createPropertyDescriptor)
}
