package com.datastax.spark.connector.datasource

class CassandraCatalogTableSpec extends CassandraCatalogSpecBase {

  "A Cassandra Catalog Table Support" should "initialize successfully" in {
    spark.sessionState.catalogManager.currentCatalog.name() should include("Catalog cassandra")
  }
}
