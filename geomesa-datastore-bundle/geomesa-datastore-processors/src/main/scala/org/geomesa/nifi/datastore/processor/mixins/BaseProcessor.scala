/***********************************************************************
 * Copyright (c) 2015-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor
package mixins

import org.apache.nifi.annotation.lifecycle.OnAdded
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.{AbstractProcessor, ProcessContext, ProcessorInitializationContext, Relationship}

import java.util.Collections

abstract class BaseProcessor extends AbstractProcessor {

  import scala.collection.JavaConverters._

  private val relationships = new java.util.HashSet[Relationship]
  private val descriptors = new java.util.ArrayList[PropertyDescriptor]

  protected def logger: ComponentLog = getLogger

  override def getRelationships: java.util.Set[Relationship] = Collections.unmodifiableSet(relationships)
  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] =
    Collections.unmodifiableList(descriptors)

  override protected def init(context: ProcessorInitializationContext): Unit = {
    relationships.clear()
    getShips.foreach(relationships.add)

    reloadDescriptors()

    logger.info(s"Props are ${descriptors.asScala.mkString(", ")}")
    logger.info(s"Relationships are ${relationships.asScala.mkString(", ")}")
  }

  @OnAdded // reload on add to pick up any sft/converter classpath changes
  def reloadDescriptors(): Unit = {
    descriptors.clear()
    getPrimaryProperties.foreach(descriptors.add)
    getSecondaryProperties.foreach(descriptors.add)
    getTertiaryProperties.foreach(descriptors.add)
    getConfigProperties.foreach(descriptors.add)
    getServiceProperties.foreach(descriptors.add)
  }

  protected def getShips: Seq[Relationship] =
    Seq(Relationships.SuccessRelationship, Relationships.FailureRelationship)


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
  protected def getSecondaryProperties: Seq[PropertyDescriptor] = Seq.empty

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
}

object BaseProcessor {

  def getFirst(context: ProcessContext, props: Seq[PropertyDescriptor]): Option[String] =
    props.toStream.flatMap(p => Option(context.getProperty(p).getValue)).headOption

  object Properties {

  }
}
