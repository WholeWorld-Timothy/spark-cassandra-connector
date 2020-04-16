package com.datastax.spark.connector.datasource

import java.util
import java.util.{Locale, Optional}

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal
import com.datastax.oss.driver.api.core.metadata.schema.{ClusteringOrder, TableMetadata}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.datastax.oss.driver.api.querybuilder.schema.{AlterTableAddColumn, AlterTableAddColumnEnd, CreateTable, OngoingPartitionKey}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.datasource.CassandraSourceUtil._
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.NamespaceChange.{RemoveProperty, SetProperty}
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, DeleteColumn}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class CassandraCatalogException(msg: String) extends Exception(msg)

/**
  * A Spark Sql Catalog for inter-operation with Cassandra
  *
  * Namespaces naturally map to C* Keyspaces, but they are always only a single
  * element deep.
  */
class CassandraCatalog extends CatalogPlugin
  with TableCatalog
  with SupportsNamespaces {

  val OnlyOneNamespace = "Cassandra only supports a keyspace name of a single level (no periods in keyspace name)"

  /**
    * The Catalog API usually deals with non-existence of tables or keyspaces by
    * throwing exceptions, so this helper should be added whenever a namespace
    * is used to quickly bail out if the namespace is not compatible with Cassandra
    */
  def checkNamespace(namespace: Array[String]): Unit = {
    if (namespace.length != 1) throw new NoSuchNamespaceException(s"$OnlyOneNamespace : $namespace")
  }

  def nameSpaceMissing(namespace: Array[String]): NoSuchNamespaceException = {
    new NoSuchNamespaceException(namespace)
    //TODO ADD Naming Suggestions
  }


  def tableMissing(namespace: Array[String], name: String): Throwable = {
    new NoSuchTableException(namespace.head, name)
    //Todo Add naming suggestions
  }

  var cassandraConnector: CassandraConnector = _
  var catalogOptions: CaseInsensitiveStringMap = _

  //Something to distinguish this Catalog from others with different hosts
  var catalogName: String = _
  var nameIdentifier: String = _

  //Add asScala to JavaOptions to make working with the driver a little smoother
  implicit class ScalaOptionConverter[T](javaOpt: Optional[T]) {
    def asScala: Option[T] =
      if (javaOpt.isPresent) Some(javaOpt.get) else None
  }

  private def getMetadata = {
    cassandraConnector.withSessionDo(_.getMetadata)
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogOptions = options
    val sparkConf = Try(SparkEnv.get.conf).getOrElse(new SparkConf())
    val connectorConf = consolidateConfs(sparkConf, options.asCaseSensitiveMap().asScala.toMap, name)
    cassandraConnector = CassandraConnector(connectorConf)
    catalogName = name
    nameIdentifier = cassandraConnector.conf.contactInfo.endPointStr()

  }

  override def name(): String = s"Catalog $catalogName For Cassandra Cluster At $nameIdentifier " //TODO add identifier here

  //Namespace Support
  private def getKeyspaceMeta(namespace: Array[String]) = {
    checkNamespace(namespace)
    getMetadata
      .getKeyspace(fromInternal(namespace.head))
      .asScala
      .getOrElse( throw nameSpaceMissing(namespace))
  }

  override def listNamespaces(): Array[Array[String]] = {
    getMetadata
      .getKeyspaces
      .asScala
      .map{ case (name, _) => Array(name.asInternal()) }
      .toArray
  }

  /**
    * Since we only allow single depth keyspace identifiers in C*
    * we always either return a single namespace or throw a NoSuchNamespaceException
    */
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    Array(Array(getKeyspaceMeta(namespace).getName().asInternal))
  }

  val ReplicationClass = "class"
  val ReplicationFactor = "replication_factor"
  val DurableWrites = "durable_writes"
  val NetworkTopologyStrategy = "networktopologystrategy"
  val SimpleStrategy = "simplestrategy"
  val IgnoredReplicationOptions = Seq(ReplicationClass, DurableWrites)

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = {
    val ksMetadata =  getKeyspaceMeta(namespace)

    (Map[String, String](
      DurableWrites -> ksMetadata.isDurableWrites.toString,
    ) ++ ksMetadata.getReplication.asScala).asJava
  }



  override def createNamespace(namespace: Array[String], metadata:java.util.Map[String, String]): Unit = {
    val ksMeta = metadata.asScala
    checkNamespace(namespace)
    if (getMetadata.getKeyspace(fromInternal(namespace.head)).asScala.isDefined) throw new NamespaceAlreadyExistsException(s"${namespace.head} already exists")
    cassandraConnector.withSessionDo{ session =>
      val createStmt = SchemaBuilder.createKeyspace(namespace.head)
      val replicationClass = ksMeta.getOrElse(ReplicationClass, throw new CassandraCatalogException(s"Creating a keyspace requires a $ReplicationClass DBOption for the replication strategy class"))
      val createWithReplication = replicationClass.toLowerCase(Locale.ROOT) match {
        case SimpleStrategy =>
          val replicationFactor = ksMeta.getOrElse(ReplicationFactor,
            throw new CassandraCatalogException(s"Need a $ReplicationFactor option with SimpleStrategy"))
          createStmt.withSimpleStrategy(replicationFactor.toInt)
        case NetworkTopologyStrategy =>
          val datacenters = (ksMeta -- IgnoredReplicationOptions).map(pair => (pair._1, pair._2.toInt: java.lang.Integer))
          createStmt.withNetworkTopologyStrategy(datacenters.asJava)
        case other => throw new CassandraCatalogException(s"Unknown keyspace replication strategy $other")
      }
      val finalCreateStmt = createWithReplication.withDurableWrites(ksMeta.getOrElse(DurableWrites, "True").toBoolean)
      session.execute(finalCreateStmt.asCql())
    }
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    checkNamespace(namespace)

    val ksMeta: mutable.Map[String, String] = changes.foldRight(loadNamespaceMetadata(namespace).asScala) {
      case (setProperty: SetProperty, metadata: mutable.Map[String, String]) =>
        metadata + (setProperty.property() -> setProperty.value)
      case (removeProperty: RemoveProperty, metadata: mutable.Map[String, String]) =>
        metadata - removeProperty.property()
    }

    val alterStart = SchemaBuilder.alterKeyspace(namespace.head)
    val alterWithDurable = alterStart.withDurableWrites(ksMeta.getOrElse(DurableWrites, "True").toBoolean)
    val replicationClass = ksMeta
      .getOrElse(ReplicationClass, throw new CassandraCatalogException(s"Creating a keyspace requires a $ReplicationClass option"))
      .split("\\.")
      .last
    val alterWithReplication = replicationClass.toLowerCase(Locale.ROOT) match {
      case SimpleStrategy =>
        val replicationFactor = ksMeta.getOrElse(ReplicationFactor,
          throw new CassandraCatalogException(s"Need a $ReplicationFactor option with SimpleStrategy"))
        alterWithDurable.withSimpleStrategy(replicationFactor.toInt)
      case NetworkTopologyStrategy =>
        val datacenters = (ksMeta -- IgnoredReplicationOptions).map(pair => (pair._1, pair._2.toInt: java.lang.Integer))
        alterWithDurable.withNetworkTopologyStrategy(datacenters.asJava)
      case other => throw new CassandraCatalogException(s"Unknown replication strategy $other")
    }

    cassandraConnector.withSessionDo( session =>
      session.execute(alterWithReplication.asCql())
    )
  }

  override def dropNamespace(namespace: Array[String]): Boolean = {
    checkNamespace(namespace)
    val keyspace = getMetadata.getKeyspace(fromInternal(namespace.head)).asScala
      .getOrElse(throw nameSpaceMissing(namespace))
    val dropResult = cassandraConnector.withSessionDo(session =>
      session.execute(SchemaBuilder.dropKeyspace(keyspace.getName).asCql()))
    dropResult.wasApplied()
  }

  //Table Support
  def getTableMetaData(ident: Identifier) = {
    val namespace = ident.namespace
    checkNamespace(namespace)
    val tableMeta = getMetadata
      .getKeyspace(fromInternal(namespace.head)).asScala
      .getOrElse(throw nameSpaceMissing(namespace))
      .getTable(fromInternal(name)).asScala
      .getOrElse(throw tableMissing(namespace, name))
    tableMeta
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
   getKeyspaceMeta(namespace)
      .getTables.asScala
      .map{case (tableName, _) => Identifier.of(namespace,tableName.asInternal())}
      .toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val tableMeta = getTableMetaData(ident)
    CassandraTable(tableMeta)
  }

  val PartitionKey = "partition_key"
  val ClusteringKey = "clustering_key"

  /**
    * Creates a Cassandra Table
    *
    * Uses properties "partition_key" and "clustering_key" to set partition key and clustering key respectively
    * for each of these properties.
    *
    * Partition key is defined as a string of comma-separated column identifiers: "col_a, col_b, col_b"
    * Clustering key is defined as a string of comma-separated column identifiers optionally marked with clustering order: "col_a.ASC, col_b.DESC"
    */
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    val tableProps = properties.asScala
    Try(getTableMetaData(ident)) match
      {
      case Success(_) => throw new TableAlreadyExistsException(ident)
      case Failure(noSuchTableException: NoSuchTableException) => //We can create this table
      case Failure(e) => throw e
      }

    val partitionKeyNames = tableProps
      .getOrElse(PartitionKey, throw new CassandraCatalogException(s"Cassandra Tables need partition keys defined in property $PartitionKey"))
      .split(",")
      .map(_.stripMargin)
      .map(fromInternal)

    val clusteringKeyNames = tableProps
      .get(ClusteringKey).toSeq
      .flatMap(value => value.split(",").map(_.stripMargin.split("\\.")))
      .map{ case arr =>
        if (arr.length != 1 || arr.length != 2)
          throw new IllegalArgumentException(s"Unable to parse clustering column $arr, too many components")
        if (arr.length == 2) {
          val clusteringOrder = Try(ClusteringOrder.valueOf(arr(1))).getOrElse(throw new IllegalArgumentException(s"Invalid clustering order found in $arr, must be ASC or DESC or blank")))
          (fromInternal(arr(0)), clusteringOrder)
        } else {
          (fromInternal(arr(0)), ClusteringOrder.ASC)
        }
      }

    val protocolVersion = cassandraConnector.withSessionDo(_.getContext.getProtocolVersion)

    val columnToType = schema.fields.map( sparkField =>
      (fromInternal(sparkField.name), sparkSqlToJavaDriverType(sparkField.dataType, protocolVersion))
    ).toMap

    val namespace = fromInternal(ident.namespace.head)
    val table = fromInternal(ident.name())

    val createTableStart:OngoingPartitionKey = SchemaBuilder.createTable(namespace, table)

    val createTableWithPk: CreateTable = partitionKeyNames.foldRight(createTableStart) { (pkName, createTable) =>
      val dataType = columnToType.getOrElse(pkName,
        throw new CassandraCatalogException(s"$pkName was defined as a partition key but it does not exist in the schema ${schema.fieldNames}"))
      createTable.withPartitionKey(pkName, dataType).asInstanceOf[OngoingPartitionKey]
    }.asInstanceOf[CreateTable]

    val createTableWithClustering = clusteringKeyNames.foldRight(createTableWithPk) { (ckName, createTable) =>
      val dataType = columnToType.getOrElse(ckName._1,
        throw new CassandraCatalogException(s"$ckName was defined as a clustering key but it does not exist in the schema ${schema.fieldNames}"))
      createTable
        .withClusteringColumn(ckName._1, dataType)
        .withClusteringOrder(ckName._1, ckName._2)
        .asInstanceOf[CreateTable]
    }

    val normalColumns = schema.fieldNames.map(fromInternal).toSet -- (clusteringKeyNames.map(_._1) ++ partitionKeyNames)

    val createTableWithColumns = normalColumns.foldRight(createTableWithClustering) { (colName, createTable) =>
      val dataType = columnToType(colName)
      createTable.withColumn(colName, dataType)
    }

    //TODO may have to add a debounce wait
    cassandraConnector.withSessionDo(_.execute(createTableWithColumns.asCql()))

    loadTable(ident)
  }

  def checkColumnName(name: Array[String]): CqlIdentifier = {
    if (name.length != 1) throw new IllegalArgumentException(s"Cassandra Column Identifiers can only have a single identifier, given $name")
    fromInternal(name.head)
  }

  def checkRemoveNormalColumn(tableMeta: TableMetadata, name: Array[String]): CqlIdentifier = {
    val colName = checkColumnName(name)
    if (!tableMeta.getColumn(colName).isPresent)
      throw new IllegalArgumentException(s"Cassandra cannot drop non-normal Columns: Tried to drop $colName")
    colName
  }

  /**
    */
  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val protocolVersion = cassandraConnector.withSessionDo(_.getContext.getProtocolVersion)
    val tableMetadata = getTableMetaData(ident)

    //Check for unsupported table changes
    changes.foreach {
      case add: AddColumn =>
      case del: DeleteColumn =>
      case other: TableChange => throw new IllegalArgumentException(s"Cassandra Catalog does not support altering a table with ${other}")
    }

    val columnsToRemove = changes.collect{ case remove: DeleteColumn =>
      checkRemoveNormalColumn(remove.fieldNames())
    }

    val dropTableStart = SchemaBuilder.alterTable(tableMetadata.getKeyspace, tableMetadata.getName)
    val dropColumnsStatement = dropTableStart.dropColumns(columnsToRemove: _*)

    val columnsToAdd = changes.collect{ case add: AddColumn =>
      (checkColumnName(add.fieldNames()), sparkSqlToJavaDriverType(add.dataType(), protocolVersion))
    }

    val addTableStart = SchemaBuilder.alterTable(tableMetadata.getKeyspace, tableMetadata.getName)
    val addColumnStatement = columnsToAdd.foldRight(addTableStart.asInstanceOf[AlterTableAddColumn]){
      case ((colName, dataType), alterBuilder) => alterBuilder.addColumn(colName, dataType)
    }.asInstanceOf[AlterTableAddColumnEnd]

    cassandraConnector.withSessionDo(_.execute(dropColumnsStatement.asCql()))
    cassandraConnector.withSessionDo(_.execute(addColumnStatement.asCql()))

    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tableMeta = getTableMetaData(ident)
    cassandraConnector.withSessionDo(_.execute(SchemaBuilder.dropTable(tableMeta.getKeyspace, tableMeta.getName).asCql())).wasApplied()
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Cassandra does not support renaming tables")
  }
}
