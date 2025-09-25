package org.geomesa.nifi.datastore.processor.service

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnDisabled, OnEnabled}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.{AbstractControllerService, ConfigurationContext}
import org.apache.nifi.processor.util.StandardValidators
import org.geomesa.nifi.datastore.processor.EnvironmentOrRegistry
import org.geomesa.nifi.datastore.processor.service.PrometheusRegistryService.{ApplicationTagProperty, PortProperty, RenameProperty}
import org.geomesa.nifi.datastore.services.MetricsRegistryService
import org.locationtech.geomesa.metrics.micrometer.prometheus.PrometheusFactory
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

@Tags(Array("geomesa", "geotools", "geo", "metrics", "prometheus"))
@CapabilityDescription("Service for publishing metrics to Prometheus")
class PrometheusRegistryService extends AbstractControllerService with MetricsRegistryService {

  import scala.collection.JavaConverters._

  private val descriptors = java.util.List.of(PortProperty, ApplicationTagProperty, RenameProperty)

  private val references = Collections.newSetFromMap(new ConcurrentHashMap[Closeable, java.lang.Boolean]())

  private var application: String = _
  private var port: Int = -1
  private var rename: Boolean = false

  override def register(): Closeable = {
    val ref = PrometheusFactory.register(port, application, rename)
    // keep track of the refs we create, just in case the calling class doesn't dispose of it correctly
    val closeable = new Closeable {
      override def close(): Unit = {
        ref.close()
        references.remove(this)
      }
    }
    references.add(closeable)
    closeable
  }

  // noinspection ScalaUnusedSymbol
  @OnEnabled
  def onEnabled(context: ConfigurationContext): Unit = {
    application = context.getProperty(ApplicationTagProperty).evaluateAttributeExpressions().getValue
    port = context.getProperty(PortProperty).evaluateAttributeExpressions().asInteger
    rename = context.getProperty(RenameProperty).evaluateAttributeExpressions().asBoolean
  }

  // noinspection ScalaUnusedSymbol
  @OnDisabled
  def onDisabled(): Unit = {
    // copy so we don't get ConcurrentModificationExceptions when refs get removed in the close method
    CloseWithLogging(Seq(references.asScala.toList: _*))
  }

  override protected def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = descriptors
}

object PrometheusRegistryService {

  val PortProperty: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("port")
      .displayName("Server port")
      .description("The port on which the Prometheus metrics will be exposed")
      .required(true)
      .addValidator(StandardValidators.PORT_VALIDATOR)
      .expressionLanguageSupported(EnvironmentOrRegistry)
      .defaultValue("9090")
      .build()

  val RenameProperty: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("rename")
      .displayName("Rename Prometheus metrics")
      .description("Rename metrics according to Prometheus standard names, instead of Micrometer names")
      .required(true)
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
      .expressionLanguageSupported(EnvironmentOrRegistry)
      .defaultValue("true")
      .build()

  val ApplicationTagProperty: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("application.name")
      .displayName("Application name tag")
      .description("Add an 'application' tag to all metrics")
      .required(true)
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
      .expressionLanguageSupported(EnvironmentOrRegistry)
      .defaultValue("nifi")
      .build()
}
