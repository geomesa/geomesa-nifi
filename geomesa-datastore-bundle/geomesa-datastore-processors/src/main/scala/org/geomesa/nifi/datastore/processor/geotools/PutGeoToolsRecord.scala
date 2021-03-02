/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor.geotools

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.geomesa.nifi.datastore.processor.records.RecordIngestProcessor

@Tags(Array("geomesa", "geo", "ingest", "records", "geotools"))
@CapabilityDescription("Ingest records into a GeoTools data store")
class PutGeoToolsRecord extends GeoToolsProcessor with RecordIngestProcessor
