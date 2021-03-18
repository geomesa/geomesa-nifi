/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.redis

import org.geomesa.nifi.datastore.processor.mixins.DataStoreProcessor
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.redis.data.RedisDataStoreFactory

abstract class RedisProcessor extends DataStoreProcessor(RedisProcessor.RedisProperties)

object RedisProcessor extends PropertyDescriptorUtils {
  private val RedisProperties = createPropertyDescriptors(RedisDataStoreFactory)
}
