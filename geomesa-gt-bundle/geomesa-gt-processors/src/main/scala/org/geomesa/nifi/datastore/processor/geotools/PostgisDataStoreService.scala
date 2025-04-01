/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.geotools

import org.apache.nifi.annotation.documentation.{CapabilityDescription, DeprecationNotice, Tags}

@deprecated("Moved to package org.geomesa.nifi.processors.gt")
@DeprecationNotice(alternatives = Array(classOf[org.geomesa.nifi.processors.gt.PostgisDataStoreService]), reason = "Moved package to align with nar structure")
@Tags(Array[String]())
@CapabilityDescription("DEPRECATED")
class PostgisDataStoreService extends org.geomesa.nifi.processors.gt.PostgisDataStoreService
