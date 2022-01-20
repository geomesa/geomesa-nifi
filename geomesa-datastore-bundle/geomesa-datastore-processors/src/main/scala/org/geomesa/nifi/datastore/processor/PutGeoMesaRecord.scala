/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}

@Tags(Array("geomesa", "geo", "ingest", "records", "accumulo", "geotools"))
@CapabilityDescription("Ingest records into GeoMesa")
class PutGeoMesaRecord extends RecordIngestProcessor
