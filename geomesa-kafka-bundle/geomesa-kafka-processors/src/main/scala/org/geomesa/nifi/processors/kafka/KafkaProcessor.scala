/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.processors.kafka

import org.apache.nifi.processor.ProcessContext
import org.geomesa.nifi.datastore.processor.DataStoreProcessor
import org.geomesa.nifi.datastore.processor.utils.PropertyDescriptorUtils
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams.{ProducerConfig, TopicPartitions, TopicReplication}
import org.locationtech.geomesa.kafka.data.{KafkaDataStoreFactory, KafkaDataStoreParams}

abstract class KafkaProcessor extends DataStoreProcessor(KafkaProcessor.KafkaProperties) {
  // set consumer count to zero to disable consuming
  override protected def getDataStoreParams(context: ProcessContext): Map[String, _] =
    super.getDataStoreParams(context) ++ Map(KafkaDataStoreParams.ConsumerCount.getName -> Int.box(0))
}

object KafkaProcessor extends PropertyDescriptorUtils {
  // note: KafkaDataStoreFactory.ParameterInfo is consumer-oriented, but we want producer properties here
  private val KafkaProperties =
    createPropertyDescriptors(KafkaDataStoreFactory) ++
        Seq(ProducerConfig, TopicPartitions, TopicReplication).map(createPropertyDescriptor)
}
