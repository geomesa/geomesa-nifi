package org.geomesa.nifi.processors

import org.testcontainers.utility.DockerImageName

package object kafka {
  val KafkaImage =
    DockerImageName.parse("apache/kafka-native")
      .withTag(sys.props.getOrElse("kafka.docker.tag", "3.9.1"))
}
