/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.geotools

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.geomesa.nifi.datastore.processor.AvroIngestProcessor

@Tags(Array("geomesa", "geo", "ingest", "avro", "geotools"))
@CapabilityDescription("Ingest GeoAvro files into a GeoTools data store")
class AvroToPutGeoTools extends GeoToolsProcessor with AvroIngestProcessor
