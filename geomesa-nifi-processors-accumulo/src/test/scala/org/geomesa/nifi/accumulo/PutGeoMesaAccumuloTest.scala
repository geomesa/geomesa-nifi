package org.geomesa.nifi.accumulo



import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.apache.nifi.util.TestRunners
import org.geomesa.nifi.geo.AvroIngest.LenientMatch
import org.geomesa.nifi.geo.{AbstractGeoIngestProcessor, AvroIngest, IngestMode}
import org.geotools.data.DataStoreFinder
import org.junit.{Assert, Test}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroDataFileWriter
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils

class PutGeoMesaAccumuloTest extends LazyLogging {
  import scala.collection.JavaConverters._

  lazy val root = s"${MiniCluster.namespace}.${getClass.getSimpleName}"

  // note the table needs to be different to prevent tests from conflicting with each other
  lazy val dsParams: Map[String, String] = Map(
    AccumuloDataStoreParams.instanceIdParam.key -> MiniCluster.cluster.getInstanceName,
    AccumuloDataStoreParams.zookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
    AccumuloDataStoreParams.userParam.key       -> MiniCluster.Users.root.name,
    AccumuloDataStoreParams.passwordParam.key   -> MiniCluster.Users.root.password
  )

  @Test
  def testAvroIngest(): Unit = {
    val catalog = s"${root}AvroIngest"
    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.tableNameParam.key, catalog)
      runner.setProperty(AbstractGeoIngestProcessor.Properties.SftName, "example")
      runner.setProperty(AbstractGeoIngestProcessor.Properties.IngestModeProp, IngestMode.AvroDataFile)
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)

      runner.enqueue(getClass.getClassLoader.getResourceAsStream("bad-example-csv.avro"))
      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 1)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.tableNameParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      logger.debug(features.mkString(";"))
      Assert.assertEquals(3, features.length)
    } finally {
      ds.dispose()
    }
  }

  @Test
  def testAvroIngestByName(): Unit = {
    val catalog = s"${root}AvroIngestByName"
    // This should be the "Good SFT" for the example-csv.avro
    val sft = SimpleFeatureTypes.createType("test", "fid:Int,name:String,age:Int,lastseen:Date,*geom:Point:srid=4326")

    // Let's make a new Avro file
    val sft2 = SimpleFeatureTypes.createType("test2", "lastseen:Date,newField:Double,age:Int,name:String,*geom:Point:srid=4326")

    val baos = new ByteArrayOutputStream()
    val writer = new AvroDataFileWriter(baos, sft2)
    val sf = new ScalaSimpleFeature(sft2, "sf2-record", Array(new Date(), new java.lang.Double(2.34), new Integer(34), "Ray", WKTUtils.read("POINT(1.2 3.4)")))
    sf.getUserData().put("geomesa.feature.visibility", "admin")
    writer.append(sf)
    writer.flush()
    writer.close()

    val is = new ByteArrayInputStream(baos.toByteArray)

    val runner = TestRunners.newTestRunner(new PutGeoMesaAccumulo())
    try {
      dsParams.foreach { case (k, v) => runner.setProperty(k, v) }
      runner.setProperty(AccumuloDataStoreParams.tableNameParam.key, catalog)
      runner.setProperty(AbstractGeoIngestProcessor.Properties.SftName, "example")
      runner.setProperty(AbstractGeoIngestProcessor.Properties.IngestModeProp, IngestMode.AvroDataFile)
      runner.setProperty(AvroIngest.AvroMatchMode, LenientMatch)
      runner.enqueue(getClass.getClassLoader.getResourceAsStream("example-csv.avro"))

      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 1)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)

      runner.enqueue(getClass.getClassLoader.getResourceAsStream("bad-example-csv.avro"))
      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 2)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)

      runner.enqueue(is)
      runner.run()
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.SuccessRelationship, 3)
      runner.assertTransferCount(AbstractGeoIngestProcessor.Relationships.FailureRelationship, 0)
    } finally {
      runner.shutdown()
    }

    val ds = DataStoreFinder.getDataStore((dsParams + (AccumuloDataStoreParams.tableNameParam.key -> catalog)).asJava)
    Assert.assertNotNull(ds)
    try {
      val sft = ds.getSchema("example")
      Assert.assertNotNull(sft)
      val features = SelfClosingIterator(ds.getFeatureSource("example").getFeatures.features()).toList
      val ray = features.find(sf => sf.getAttribute("name") == "Ray")
      Assert.assertTrue(ray.map(!_.getUserData.isEmpty).get)
      logger.debug(features.mkString(";"))
      Assert.assertEquals(7, features.length)
    } finally {
      ds.dispose()
    }
  }

}
