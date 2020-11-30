/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.behavior.{InputRequirement, SupportsBatching}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.context.PropertyContext
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.{AbstractProcessor, ProcessContext, ProcessorInitializationContext, Relationship}
import org.geotools.data.{DataStore, DataStoreFinder}

/**
 * Abstract processor that uses a data store
 *
 * @param dataStoreProperties properties exposed through NiFi used to load the data store
 */
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
abstract class DataStoreProcessor(dataStoreProperties: Seq[PropertyDescriptor]) extends AbstractProcessor {

  import Relationships.{FailureRelationship, SuccessRelationship}
  import org.geomesa.nifi.datastore.processor.DataStoreIngestProcessor.Properties.NifiBatchSize
  import org.geomesa.nifi.datastore.processor.DataStoreProcessor.Properties.ExtraClasspaths

  import scala.collection.JavaConverters._

  private var relationships: Set[Relationship] = _
  private var descriptors: Seq[PropertyDescriptor] = _

  protected def logger: ComponentLog = getLogger

  override def getRelationships: java.util.Set[Relationship] = relationships.asJava
  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = descriptors.asJava

  override protected def init(context: ProcessorInitializationContext): Unit = {
    loadDescriptors()
    relationships = Set(SuccessRelationship, FailureRelationship)
    logger.info(s"Props are ${descriptors.mkString(", ")}")
    logger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  protected def loadDescriptors(): Unit = {
    descriptors =
        getProcessorProperties ++
            getExtraProperties ++
            dataStoreProperties ++
            getConfigProperties ++
            getServiceProperties
  }

  protected def loadDataStore(context: ProcessContext): DataStore = {
    val props = getDataStoreParams(context)
    lazy val safeToLog = {
      val sensitive = context.getProperties.keySet().asScala.collect { case p if p.isSensitive => p.getName }
      props.map { case (k, v) => s"$k -> ${if (sensitive.contains(k)) { "***" } else { v }}" }
    }
    logger.trace(s"DataStore properties: ${safeToLog.mkString(", ")}")
    val ds = DataStoreFinder.getDataStore(props.asJava)
    require(ds != null, "Could not load datastore using provided parameters")
    ds
  }

  /**
   * Get params for looking up the data store, based on the current processor configuration
   *
   * @param context context
   * @return
   */
  protected def getDataStoreParams(context: ProcessContext): Map[String, _] =
    DataStoreProcessor.getDataStoreParams(context, dataStoreProperties)

  /**
   * Get the main processor params - these will come first in the UI
   *
   * @return
   */
  protected def getProcessorProperties: Seq[PropertyDescriptor] = Seq.empty

  /**
   * Get secondary processor params - these will come second in the UI
   *
   * @return
   */
  protected def getExtraProperties: Seq[PropertyDescriptor] = Seq(ExtraClasspaths)

  /**
   * Get params for less common configs - these will come fourth in the UI, after the data store params
   *
   * @return
   */
  protected def getConfigProperties: Seq[PropertyDescriptor] = Seq(NifiBatchSize)

  /**
   * Get params for configuration services - these will come fifth in the UI, after config params
   *
   * @return
   */
  protected def getServiceProperties: Seq[PropertyDescriptor] = Seq.empty
}

object DataStoreProcessor {

  /**
   * Get data store parameter map based on a nifi context
   *
   * @param context context
   * @param props property descriptors corresponding to the data store factory params
   * @return
   */
  def getDataStoreParams(context: PropertyContext, props: Seq[PropertyDescriptor]): Map[String, _] = {
    val builder = Map.newBuilder[String, AnyRef]
    props.foreach { p =>
      val property = {
        val prop = context.getProperty(p)
        if (p.isExpressionLanguageSupported) { prop.evaluateAttributeExpressions() }  else { prop }
      }
      val value = property.getValue
      if (value != null) {
        builder += p.getName -> value
      }
    }
    builder.result
  }

  object Properties {
    val ExtraClasspaths: PropertyDescriptor =
      new PropertyDescriptor.Builder()
          .name("ExtraClasspaths")
          .required(false)
          .description("Add additional resources to the classpath")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .dynamicallyModifiesClasspath(true)
          .build()
  }
}
