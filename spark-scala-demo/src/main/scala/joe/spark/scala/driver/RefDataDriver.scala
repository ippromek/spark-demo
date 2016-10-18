package joe.spark.scala.driver

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import joe.spark.scala.processor.RefDataProcessor
import joe.spark.scala._

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
      getOrCreate()

    val countryDs = RefDataProcessor.readCountryRefData(sparkSession, conf) 
    println("COUNTRY count = " + countryDs.count())
    countryDs.show()

    val stateDs = RefDataProcessor.readStateRefData(sparkSession, conf) 
    println("STATE count = " + stateDs.count())
    stateDs.show()
        
    sparkSession.stop()

  }

}