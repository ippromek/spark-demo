package joe.spark.scala.processor

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import joe.spark.scala._

object RefDataProcessor {
  
  def readStateRefData(sparkSession: SparkSession, config: Config): Dataset[State] = {
    import sparkSession.implicits._      

    val dataframe = readRefData(sparkSession, config, "STATE")
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
    val dataframe = readRefData(sparkSession, config, "COUNTRY")
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