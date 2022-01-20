/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import org.apache.nifi.annotation.documentation.{DeprecationNotice, Tags}
import org.geomesa.nifi.datastore.processor.{AvroIngestProcessor, AvroToPutGeoMesa}

@Tags(Array("geomesa", "geo", "ingest", "avro", "kafka", "stream", "streaming", "geotools"))
@DeprecationNotice(
  alternatives = Array(classOf[KafkaDataStoreService], classOf[AvroToPutGeoMesa]),
  reason = "Replaced with controller service for data store connections")
class AvroToPutGeoMesaKafka extends KafkaProcessor with AvroIngestProcessor
