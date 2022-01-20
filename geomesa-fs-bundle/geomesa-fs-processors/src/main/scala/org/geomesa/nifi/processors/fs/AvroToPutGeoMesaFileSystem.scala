/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.fs

import org.apache.nifi.annotation.documentation.{DeprecationNotice, Tags}
import org.geomesa.nifi.datastore.processor.{AvroIngestProcessor, AvroToPutGeoMesa}

@Tags(Array("geomesa", "geo", "ingest", "avro", "hdfs", "s3", "geotools"))
@DeprecationNotice(
  alternatives = Array(classOf[FileSystemDataStoreService], classOf[AvroToPutGeoMesa]),
  reason = "Replaced with controller service for data store connections")
class AvroToPutGeoMesaFileSystem extends FileSystemIngestProcessor with AvroIngestProcessor
