/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor
package mixins

import java.util.Collections

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.{AbstractProcessor, ProcessContext, ProcessorInitializationContext, Relationship}
import org.geomesa.nifi.datastore.processor.mixins.BaseProcessor.FeatureTypeDecorator
import org.opengis.feature.simple.SimpleFeatureType

@InputRequirement(Requirement.INPUT_REQUIRED)
abstract class BaseProcessor extends AbstractProcessor {

  import BaseProcessor.Properties.ExtraClasspaths

  import scala.collection.JavaConverters._

  private val relationships = new java.util.HashSet[Relationship]
  private val descriptors = new java.util.ArrayList[PropertyDescriptor]

  protected def logger: ComponentLog = getLogger

  override def getRelationships: java.util.Set[Relationship] = Collections.unmodifiableSet(relationships)
  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] =
    Collections.unmodifiableList(descriptors)

  override protected def init(context: ProcessorInitializationContext): Unit = {
    relationships.clear()
    relationships.add(Relationships.SuccessRelationship)
    relationships.add(Relationships.FailureRelationship)

    reloadDescriptors()

    logger.info(s"Props are ${descriptors.asScala.mkString(", ")}")
    logger.info(s"Relationships are ${relationships.asScala.mkString(", ")}")
  }

  protected def reloadDescriptors(): Unit = {
    descriptors.clear()
    getPrimaryProperties.foreach(descriptors.add)
    getSecondaryProperties.foreach(descriptors.add)
    getTertiaryProperties.foreach(descriptors.add)
    getConfigProperties.foreach(descriptors.add)
    getServiceProperties.foreach(descriptors.add)
  }

  /**
   * Get the main processor params - these will come first in the UI
   *
   * @return
   */
  protected def getPrimaryProperties: Seq[PropertyDescriptor] = Seq.empty

  /**
   * Get secondary processor params - these will come second in the UI
   *
   * @return
   */
  protected def getSecondaryProperties: Seq[PropertyDescriptor] = Seq(ExtraClasspaths)

  /**
   * Get tertiary processor params - these will come third in the UI
   *
   * @return
   */
  protected def getTertiaryProperties: Seq[PropertyDescriptor] = Seq.empty

  /**
   * Get params for less common configs - these will come fourth in the UI, after the data store params
   *
   * @return
   */
  protected def getConfigProperties: Seq[PropertyDescriptor] = Seq.empty

  /**
   * Get params for configuration services - these will come fifth in the UI, after config params
   *
   * @return
   */
  protected def getServiceProperties: Seq[PropertyDescriptor] = Seq.empty

  /**
   * Provides the processor a chance to configure a feature type based on the processor config
   *
   * @return
   */
  protected def decorator: Option[FeatureTypeDecorator] = None
}

object BaseProcessor {

  trait FeatureTypeDecorator {

    /**
     * Properties used to decorate the feature type, will be provided in the decorate method
     *
     * @return
     */
    def properties: Seq[PropertyDescriptor]

    /**
     * Decorate the feature type based on configured properties
     *
     * @param sft simple feature type
     * @param properties evaluated properties, as defined by the `properties` method
     * @return
     */
    def decorate(sft: SimpleFeatureType, properties: Map[PropertyDescriptor, String]): SimpleFeatureType
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
