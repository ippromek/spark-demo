package joe.spark.scala.driver

import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.cassandra._
import com.datastax.spark
import com.datastax.spark._
import com.datastax.spark.connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnector._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
 *
 * spark-submit --class joe.spark.scala.driver.CassandraDriver <path-to-jar>\spark-scala-demo-0.0.1-SNAPSHOT.jar
 *
 */

object CassandraDriver {

  def main(args: Array[String]) {
    println("Starting CassandraDriver")
    println("Using conf file: " + CASSANDRA_DRIVER_CONF_FILE)
    val conf = ConfigFactory.load(CASSANDRA_DRIVER_CONF_FILE)

    val appName = conf.getString("cassandradriver.appName")
    val sparkMaster = conf.getString("cassandradriver.sparkMaster")
    val sparkWarehouseDir = conf.getString("cassandradriver.sparkWarehouseDir")
    val sparkLocalDir = conf.getString("cassandradriver.sparkLocalDir")
    val cassandraHost = conf.getString("cassandradriver.host")
    val cassandraPort = conf.getInt("cassandradriver.port")

    //    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    //    val sc = new SparkContext("local[*]", "test", conf)
    //    val table = sc.cassandraTable("test", "my_table")
    //    println(table.count)

    val sparkSession = SparkSession.builder.
      master(sparkMaster).
      appName(appName).
      config("spark.sql.warehouse.dir", sparkWarehouseDir).
      getOrCreate()

    val df = sparkSession.sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "my_table", "keyspace" -> "test")).load()

    println("Table size: " + df.count())

    sparkSession.stop()

  }

}