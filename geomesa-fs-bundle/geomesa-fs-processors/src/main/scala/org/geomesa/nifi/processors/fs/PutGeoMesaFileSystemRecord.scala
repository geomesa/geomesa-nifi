/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.fs

import org.apache.nifi.annotation.documentation.{DeprecationNotice, Tags}
import org.geomesa.nifi.datastore.processor.{PutGeoMesaRecord, RecordIngestProcessor}

@Tags(Array("geomesa", "geo", "ingest", "records", "hdfs", "s3", "geotools"))
@DeprecationNotice(
  alternatives = Array(classOf[FileSystemDataStoreService], classOf[PutGeoMesaRecord]),
  reason = "Replaced with controller service for data store connections")
class PutGeoMesaFileSystemRecord extends FileSystemIngestProcessor with RecordIngestProcessor
