/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.hbase

import org.geomesa.nifi.datastore.processor.ConverterIngestProcessor

class AvroToPutGeoMesaHBase extends GeoMesaHBaseProcessor with ConverterIngestProcessor
