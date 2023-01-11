/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors

import org.apache.nifi.components.PropertyDescriptor
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.geotools.data.DataAccessFactory.Param

package object gt {

  trait JdbcPropertyDescriptorUtils extends PropertyDescriptorUtils {
    override def createPropertyDescriptor(param: Param): PropertyDescriptor = {
      val descriptor = super.createPropertyDescriptor(param)
      if (param.getName == "dbtype") {
        // override dbtype to be unmodifiable, since we invoke the data store factory directly
        new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(descriptor)
            .allowableValues(param.getDefaultValue.toString)
            .build()
      } else {
        descriptor
      }
    }
  }
}
