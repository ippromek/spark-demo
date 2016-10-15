package joe.spark.scala.driver

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType

/**
 * 
 * spark-submit --class joe.spark.scala.driver.RefDataDriver <path-to-jar>\spark-scala-demo-0.0.1-SNAPSHOT.jar
 *
 */

object RefDataDriver {

  def main(args: Array[String]) {

    println("Starting RefDataDriver")
    println("Using conf file: " + REF_DATA_DRIVER_CONF_FILE)
    val conf = ConfigFactory.load(REF_DATA_DRIVER_CONF_FILE)
    
    val appName = conf.getString("refdatadriver.appName")
    val sparkMaster = conf.getString("refdatadriver.sparkMaster")
    val sparkWarehouseDir = conf.getString("refdatadriver.sparkWarehouseDir")
    val sparkLocalDir = conf.getString("refdatadriver.sparkLocalDir")

    val sparkSession = SparkSession.builder.
      master(sparkMaster).
      appName(appName).
      config("spark.sql.warehouse.dir", sparkWarehouseDir).
      //        config("spark.local.dir", sparkLocalDir).
      getOrCreate()

    //    sparkSession.conf.getAll.foreach { x => println(x) }

    val countryDs = readCountryRefData(sparkSession, conf) 
    println("COUNTRY count = " + countryDs.count())
    countryDs.show()
//    countryDf.foreach { x => println(x.getString(x.fieldIndex("NAME"))) }

    val stateDs = readStateRefData(sparkSession, conf) 
    println("STATE count = " + stateDs.count())
    stateDs.show()
//    stateDf.foreach { x => println(x.getString(x.fieldIndex("NAME"))) }
    
    
//    val mergedDs = stateDs.join(right, joinExprs, joinType)
    sparkSession.stop()

  }

  def readStateRefData(sparkSession: SparkSession, config: Config): Dataset[State] = {
    import sparkSession.implicits._      

    val dataframe = readRefData(sparkSession, config, "(SELECT * from STATE)")
    dataframe.printSchema()
    
    // When using the Oracle JDBC driver, all numeric fields are read as
    // decimal values. We need to cast the id columns to longs
    val modDataframe = dataframe.
        withColumn("ID", dataframe.col("ID").cast(LongType)).
        withColumn("COUNTRY_ID", dataframe.col("COUNTRY_ID").cast(LongType)).
        withColumnRenamed("COUNTRY_ID","COUNTRYID")  
    modDataframe.printSchema()
    val dataset = modDataframe.as[State]
    dataset
  }

  def readCountryRefData(sparkSession: SparkSession, config: Config): Dataset[Country] = {
    import sparkSession.implicits._      
    val dataframe = readRefData(sparkSession, config, "(SELECT * from COUNTRY)")
    dataframe.printSchema()

    // When using the Oracle JDBC driver, all numeric fields are read as
    // decimal values. We need to cast the id columns to longs
    val modDataframe = dataframe.
        withColumn("ID", dataframe.col("ID").cast(LongType))
    modDataframe.printSchema()
    val dataset = modDataframe.as[Country]
    dataset
  }
  
  def readRefData(sparkSession: SparkSession, config: Config, dbTable: String): DataFrame = {

    val jdbcDf = sparkSession.read
      .format("jdbc")
      .option("driver", config.getString("refdatadriver.db.driver"))
      .option("url", config.getString("refdatadriver.db.url"))
      .option("user", config.getString("refdatadriver.db.user"))
      .option("password", config.getString("refdatadriver.db.password"))
      .option("dbtable", dbTable)
      .load()

    jdbcDf  
  }

}