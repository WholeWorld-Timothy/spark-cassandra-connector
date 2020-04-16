package com.datastax.spark.connector.datasource

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._
import org.scalatest.BeforeAndAfterAll

class CassandraCatalogSpecBase
  extends SparkCassandraITFlatSpecBase
    with DefaultCluster
    with BeforeAndAfterAll {

  override def conn: CassandraConnector = CassandraConnector(sparkConf)

  val defaultKs = "catalogtestks"

  def getMetadata() = {
    conn.withSessionDo(_.getMetadata)
  }

  def dropKeyspace(name: String) = {
    conn.withSessionDo(_.execute(s"DROP KEYSPACE IF EXISTS $name"))
  }

  def waitForKeyspaceToExist(keyspace: String, exist: Boolean) = {
    eventually(getMetadata().getKeyspace(keyspace).isPresent shouldBe exist)
  }

  implicit val patienceConfig = PatienceConfig(scaled(5 seconds), scaled(200 millis))

  val catalogName = "cassandra"

  override def beforeClass: Unit = {
    super.beforeClass
    spark.conf.set(s"spark.sql.catalog.$catalogName", classOf[CassandraCatalog].getCanonicalName)
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "cassandra")
  }

}
