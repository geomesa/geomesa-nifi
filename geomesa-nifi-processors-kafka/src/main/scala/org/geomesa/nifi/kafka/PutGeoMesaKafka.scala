/***********************************************************************
 * Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.kafka

import java.util

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor._
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Properties._
import org.geomesa.nifi.geo.{AbstractGeoIngestProcessor, IngestMode}
import org.geomesa.nifi.kafka.PutGeoMesaKafka._
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.{KafkaDataStoreFactoryParams => KDSP}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "kafka", "streaming", "stream", "geo", "ingest", "convert", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
class PutGeoMesaKafka extends AbstractGeoIngestProcessor {

  protected override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)

    descriptors = (getPropertyDescriptors ++ KdsNifiProps).asJava
    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  // Abstract
  override protected def getDataStore(context: ProcessContext): DataStore = {
    val props = KdsNifiProps.map { p =>
      p.getName -> context.getProperty(p.getName).getValue
    }.filter(_._2 != null).map { case (p, v) =>
      getLogger.trace(s"DataStore Properties: $p => $v")
      p -> {
        KdsGTProps.find(_.getName == p).head.getType match {
          case x if x.isAssignableFrom(classOf[java.lang.Integer]) => v.toInt
          case x if x.isAssignableFrom(classOf[java.lang.Long])    => v.toLong
          case x if x.isAssignableFrom(classOf[java.lang.Boolean]) => v.toBoolean
          case _                                                   => v
        }
      }
    } :+ (KDSP.ConsumerCount -> 0) // only producing

    DataStoreFinder.getDataStore(props.toMap.asJava)
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {

    val validationFailures = new util.ArrayList[ValidationResult]()

    // If using converters check for params relevant to that
    def useConverter = validationContext.getProperty(IngestModeProp).getValue == IngestMode.Converter
    if (useConverter) {
      // make sure either a sft is named or written
      val sftNameSet = validationContext.getProperty(SftName).isSet
      val sftSpecSet = validationContext.getProperty(SftSpec).isSet
      if (!sftNameSet && !sftSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a simple feature type by name or spec")
          .build)

      val convNameSet = validationContext.getProperty(ConverterName).isSet
      val convSpecSet = validationContext.getProperty(ConverterSpec).isSet
      if (!convNameSet && !convSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a converter by name or spec")
          .build
        )
    }

    validationFailures
  }

}

object PutGeoMesaKafka {
  val KdsGTProps = List(
    KDSP.Brokers,
    KDSP.Zookeepers,
    KDSP.ZkPath,
    KDSP.TopicPartitions,
    KDSP.TopicReplication
  )

  val KdsNifiProps: List[PropertyDescriptor] = KdsGTProps.map { p =>
    new PropertyDescriptor.Builder()
      .name(p.getName)
      .description(p.getDescription.toString)
      .required(p.isRequired)
      .defaultValue(if (p.getDefaultValue != null) p.getDefaultValue.toString else null)
      .addValidator(p.getType match {
        case x if x.isAssignableFrom(classOf[java.lang.Integer]) => StandardValidators.INTEGER_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.Long])    => StandardValidators.LONG_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.Boolean]) => StandardValidators.BOOLEAN_VALIDATOR
        case x if x.isAssignableFrom(classOf[java.lang.String])  => StandardValidators.NON_EMPTY_VALIDATOR
        case _                                                   => StandardValidators.NON_EMPTY_VALIDATOR
      })
      .build()
  }

}
