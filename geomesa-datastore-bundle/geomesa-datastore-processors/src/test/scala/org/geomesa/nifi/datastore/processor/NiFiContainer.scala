/***********************************************************************
 * Copyright (c) 2015-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.processor

import com.github.dockerjava.api.command.InspectContainerResponse
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geomesa.nifi.datastore.processor.NiFiContainer.findNar
import org.locationtech.geomesa.utils.io.WithClose
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.{BindMode, GenericContainer}
import org.testcontainers.utility.{DockerImageName, PathUtils}

import java.io.{ByteArrayInputStream, File, FileOutputStream, InputStream}
import java.nio.file.{Files, Path, Paths}
import java.util.regex.Pattern

class NiFiContainer(image: DockerImageName) extends GenericContainer[NiFiContainer](image) {

  def this() = this(NiFiContainer.ImageName)

  withExposedPorts(8443)
  withEnv("SINGLE_USER_CREDENTIALS_USERNAME", "nifi")
  withEnv("SINGLE_USER_CREDENTIALS_PASSWORD", "nifipassword")
  withEnv("NIFI_SENSITIVE_PROPS_KEY", "supersecretkey")
  waitingFor(Wait.forLogMessage(s".*${Pattern.quote(NiFiContainer.startupMessage(image))}.*", 1))
  withCreateContainerCmdModifier(cmd => cmd.withEntrypoint("/entrypoint.sh"))

  mountClasspathResource("docker/entrypoint.sh", "/entrypoint.sh", executable = true)
  mountClasspathResource("docker/reference.conf", "/opt/nifi/nifi-current/conf/reference.conf")
  mountClasspathResource("docker/20180101000000.export.CSV", "/ingest/20180101000000.export.CSV")

  withNarByName("datastore-services-api")
  withNarByName("datastore-services")

  withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("nifi")))

  /**
   * Configures a default test flow. The flow will ingest data with three different processors:
   * PutGeoMesa, PutGeoMesaRecord, and AvroToPutGeoMesa
   *
   * @param narName name of the geomesa nar used to ingest data, i.e. `kafka` or `accumulo21`
   * @return
   */
  def withDefaultIngestFlow(narName: String): NiFiContainer = {
    withNarByPath(findNar(narName))
    mountFile(getClass.getClassLoader.getResourceAsStream("docker/ingest-flow.json"), "/flow.json")
  }

  /**
   * Configures a test flow from a classpath resource. Flow file must be uncompressed json.
   *
   * @param name name of the flow file
   * @return
   */
  def withFlowFromClasspath(name: String = "flow.json"): NiFiContainer =
    mountClasspathResource(name, "/flow.json")

  /**
   * Mounts a nar by name
   *
   * @param name name of the nar to mount, i.e. `kafka` or `accumulo21`
   * @return
   */
  def withNarByName(name: String): NiFiContainer = withNarByPath(NiFiContainer.findNar(name))

  /**
   * Mounts a nar by path
   *
   * @param narHostPath path to the nar
   * @return
   */
  def withNarByPath(narHostPath: String): NiFiContainer = {
    val extensions = if (image.getVersionPart.startsWith("1.")) { "extensions"} else { "nar_extensions"}
    mountFile(narHostPath, s"/opt/nifi/nifi-current/$extensions/${new File(narHostPath).getName}")
  }

  /**
   * Enables JVM remote debugging
   *
   * @return
   */
  def withJvmDebug(): NiFiContainer = withEnv("NIFI_JVM_DEBUGGER", "true")

  /**
   * Mounts a classpath resource into the container
   *
   * @param name name of the resource
   * @param mount mount path
   * @param executable executable flag
   * @return
   */
  private def mountClasspathResource(name: String, mount: String, executable: Boolean = false): NiFiContainer =
    WithClose(getClass.getClassLoader.getResourceAsStream(name))(mountFile(_, mount, executable))

  /**
   * Mounts a file into the container
   *
   * @param file file contents
   * @param mount mount path in the container
   * @param executable executable flag
   * @return
   */
  private def mountFile(file: InputStream, mount: String, executable: Boolean = false): NiFiContainer = {
    val tmp = Files.createTempDirectory("gm-nifi")
    sys.addShutdownHook(PathUtils.recursiveDeleteDir(tmp))
    val name = new File(mount).getName
    val out = tmp.resolve(name)
    val outFile = out.toFile
    WithClose(new FileOutputStream(outFile))(out => IOUtils.copy(file, out))
    if (executable) {
      outFile.setExecutable(true, false)
    }
    mountFile(outFile.getAbsolutePath, mount)
  }

  /**
   * Mount a file into the container
   *
   * @param hostPath path to the file
   * @param mount mount path in the container
   * @return
   */
  private def mountFile(hostPath: String, mount: String): NiFiContainer = {
    logger.debug(s"Mounting $hostPath:$mount")
    withFileSystemBind(hostPath, mount, BindMode.READ_ONLY)
  }

  override protected def containerIsStarted(containerInfo: InspectContainerResponse): Unit = {
    super.containerIsStarted(containerInfo)
    logger.info(s"The NiFi UI is available locally at: http://$getHost:$getFirstMappedPort/nifi")
  }
}

object NiFiContainer extends LazyLogging {

  val ImageName =
    DockerImageName.parse("apache/nifi")
        .withTag(sys.props.getOrElse("nifi.it.version", "2.3.0"))

  // type names created by the default ingest flow
  val DefaultIngestTypes: Seq[String] = Seq("gdelt-nifi", "gdelt-nifi-avro", "gdelt-nifi-records")

  /**
   * Write a temp file to disk, with automatic cleanup after jvm shutdown
   *
   * @param name filename
   * @param bytes file contents
   * @return path to the file on disk
   */
  def writeTempFile(name: String, bytes: Array[Byte]): Path =
    writeTempFile(name, new ByteArrayInputStream(bytes))

  /**
   * Write a temp file to disk, with automatic cleanup after jvm shutdown
   *
   * @param name filename
   * @param is file contents
   * @return path to the file on disk
   */
  def writeTempFile(name: String, is: InputStream): Path = {
    val tmp = Files.createTempDirectory("gm-nifi")
    sys.addShutdownHook(PathUtils.recursiveDeleteDir(tmp))
    val out = tmp.resolve(name)
    WithClose(new FileOutputStream(out.toFile))(IOUtils.copy(is, _))
    out
  }

  /**
   * Get the container startup message
   *
   * @param image image
   * @return
   */
  def startupMessage(image: DockerImageName): String =
    if (image.getVersionPart.startsWith("1.")) { "NiFi has started." } else { "Started Application in " }

  /**
   * Tries to find the path to a nar in this repository
   *
   * @param name name of the nar, i.e. 'kafka' or 'datastore-services'
   * @return path to the nar on disk
   */
  private def findNar(name: String): String = {
    def fail(msg: String = null): Unit =
      throw new RuntimeException(
        s"Could not load geomesa-$name-nar from classpath${Option(msg).map(": " + _).getOrElse("")}")

    val url = getClass.getClassLoader.getResource("nar.properties")
    if (url == null) {
      fail("could not load nar.properties")
    }
    val uri = url.toURI
    logger.debug("NAR lookup: " + uri)
    // file:/.../geomesa-*-bundle/geomesa-*-nar/target/test-classes/nar.properties
    val baseDir = Paths.get(uri).toFile.getParentFile.getParentFile.getParentFile.getParentFile.getParentFile
    val bundleDirs = baseDir.listFiles((file: File) => file.isDirectory && file.getName.matches("geomesa-.*-bundle"))
    val narDirs = bundleDirs.flatMap { dir =>
      dir.listFiles((file: File) => file.isDirectory && file.getName == s"geomesa-$name-nar")
    }
    val targetDirs = narDirs.flatMap { dir =>
      dir.listFiles((file: File) => file.isDirectory && file.getName == "target")
    }
    val nars = targetDirs.flatMap { dir =>
      dir.listFiles((file: File) => file.isFile && file.getName.startsWith(s"geomesa-$name-nar") && file.getName.endsWith(".nar"))
    }
    if (nars.isEmpty) {
      fail(s"could not locate geomesa-$name-nar")
    } else if (nars.lengthCompare(1) > 0) {
      fail(s"located multiple geomesa-$name-nar files: ${nars.map(_.getAbsolutePath).mkString("; ")}")
    }

    nars.head.getAbsolutePath
  }
}
