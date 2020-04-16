package com.datastax.spark.connector.datasource

import java.util.Locale

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.`type`.DataTypes._
import com.datastax.spark.connector.util.{ConfigParameter, DeprecatedConfigParameter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{BooleanType => SparkSqlBooleanType, DataType => SparkSqlDataType, DateType => SparkSqlDateType, DecimalType => SparkSqlDecimalType, DoubleType => SparkSqlDoubleType, FloatType => SparkSqlFloatType, MapType => SparkSqlMapType, TimestampType => SparkSqlTimestampType, _}

object CassandraSourceUtil {

  /**
    * Consolidate Cassandra conf settings in the order of
    * table level -> keyspace level -> cluster level ->
    * default. Use the first available setting. Default
    * settings are stored in SparkConf.
    */
  def consolidateConfs(
    sparkConf: SparkConf,
    sqlConf: Map[String, String],
    cluster: String = "default",
    keyspace: String = "",
    tableConf: Map[String, String] = Map.empty) : SparkConf = {

    //Default settings
    val conf = sparkConf.clone()
    val AllSCCConfNames = (ConfigParameter.names ++ DeprecatedConfigParameter.names)
    //Keyspace/Cluster level settings
    for (prop <- AllSCCConfNames) {
      val value = Seq(
        tableConf.get(prop.toLowerCase(Locale.ROOT)), //tableConf is actually a caseInsensitive map so lower case keys must be used
        sqlConf.get(s"$cluster:$keyspace/$prop"),
        sqlConf.get(s"$cluster/$prop"),
        sqlConf.get(s"default/$prop"),
        sqlConf.get(prop)).flatten.headOption
      value.foreach(conf.set(prop, _))
    }
    //Set all user properties
    conf.setAll(tableConf -- AllSCCConfNames)
    conf
  }

  def sparkSqlToJavaDriverType(
    dataType: SparkSqlDataType,
    protocolVersion: ProtocolVersion = ProtocolVersion.DEFAULT): DataType = {

    def unsupportedType() = throw new IllegalArgumentException(s"Unsupported type: $dataType")

    val pvGt4 = (protocolVersion.getCode >= ProtocolVersion.V4.getCode)

    dataType match {
      case ByteType => if (pvGt4) TINYINT else INT
      case ShortType => if (pvGt4) SMALLINT else INT
      case IntegerType => INT
      case LongType => BIGINT
      case SparkSqlFloatType => FLOAT
      case SparkSqlDoubleType => DOUBLE
      case StringType => TEXT
      case BinaryType => BLOB
      case SparkSqlBooleanType => BOOLEAN
      case SparkSqlTimestampType => TIMESTAMP
      case SparkSqlDateType => if (pvGt4) DATE else TIMESTAMP
      case SparkSqlDecimalType() => DECIMAL
      case ArrayType(sparkSqlElementType, containsNull) =>
        val argType = sparkSqlToJavaDriverType(sparkSqlElementType)
        DataTypes.listOf(argType)
      case SparkSqlMapType(sparkSqlKeyType, sparkSqlValueType, containsNull) =>
        val keyType = sparkSqlToJavaDriverType(sparkSqlKeyType)
        val valueType = sparkSqlToJavaDriverType(sparkSqlValueType)
        DataTypes.mapOf(keyType, valueType)
      case _ =>
        unsupportedType()
    }
  }
}
