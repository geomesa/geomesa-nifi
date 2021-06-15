/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import org.apache.nifi.annotation.documentation.{DeprecationNotice, Tags}
import org.geomesa.nifi.datastore.processor.{ConverterIngestProcessor, PutGeoMesa}

@Tags(Array("geomesa", "geo", "ingest", "convert", "kafka", "stream", "streaming", "geotools"))
@DeprecationNotice(
  alternatives = Array(classOf[KafkaDataStoreService], classOf[PutGeoMesa]),
  reason = "Replaced with controller service for data store connections")
class PutGeoMesaKafka extends KafkaProcessor with ConverterIngestProcessor
