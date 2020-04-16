package com.datastax.spark.connector.datasource

import java.util

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class CassandraTable(val metadata: TableMetadata) extends Table {
  override def name(): String = metadata.getName.asInternal()

  override def schema(): StructType = StructType.apply(Seq.empty)

  override def capabilities(): util.Set[TableCapability] = Set.empty[TableCapability].asJava
}

object CassandraTable {
  def apply(metadata: TableMetadata): CassandraTable = {
    new CassandraTable(metadata)
  }
}
