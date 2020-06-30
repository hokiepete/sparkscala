package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** Find the maximum temperature by weather station for a year */
object MaxTemperatures {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val date = fields(1)
    val entryType = fields(2)
    val precip = fields(3).toFloat
    (date, entryType, precip)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
   Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxTemperatures")
    
    val lines = sc.textFile("../1800.csv")
    val parsedLines = lines.map(parseLine)
    val maxTemps = parsedLines.filter(x => x._2 == "PRCP")
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    val maxTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y))
    val results = maxTempsByStation.collect()
    val maxTemp = results.maxBy(_._2)
    val date = maxTemp._1
    val precip = maxTemp._2
    println(s"max Precipitation of $precip on $date")
    /*
    for (result <- results.sorted) {
       
       val formattedPrecip = f"$precip%.2f"
       println(s"$date max Precipitation: $formattedPrecip") 
    }
    */
  }
}