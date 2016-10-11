package joe.spark.scala

import org.joda.time.LocalDate

package object driver {
  final case class StockRecord(exchange: String, 
                      symbol: String, 
                      date: LocalDate, 
                      openPrice: Double, 
                      highPrice: Double, 
                      lowPrice: Double, 
                      closePrice: Double, 
                      volume: Int, 
                      adjClosePrice: Double)
}