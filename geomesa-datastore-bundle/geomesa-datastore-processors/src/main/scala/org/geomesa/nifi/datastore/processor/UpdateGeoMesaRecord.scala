/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching, WritesAttribute, WritesAttributes}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}

@Tags(Array("geomesa", "geo", "update", "records", "geotools"))
@CapabilityDescription("Update existing features in GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes(
  Array(
    new WritesAttribute(attribute = "geomesa.ingest.successes", description = "Number of features updated successfully"),
    new WritesAttribute(attribute = "geomesa.ingest.failures", description = "Number of features with errors")
  )
)
@SupportsBatching
class UpdateGeoMesaRecord extends RecordUpdateProcessor
