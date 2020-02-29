package com.cebd.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max

/** Find the minimum temperature by weather station */
object MostWeatherChangePK {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("MinTemperatures").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named MinTemperatures
    // alternative: val sc = new SparkContext("local[*]", "MinTemperatures")
    val sc = new SparkContext(conf)
    
    // Read each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    val totalsByStation = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._3 + y._3))
    
    // Filter out all but TMAX entries
    //val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    
    // Convert to (stationID, temperature)
    //val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    
    // Reduce by stationID retaining the Maximum temperature found
    val maxchangeByStation = totalsByStation.reduceByKey( (x,y) => max(x,y))
    
    // Collect, format, and print the results
    val results = maxchangeByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station with most temperature changes: $formattedTemp") 
    }
      
  }
}