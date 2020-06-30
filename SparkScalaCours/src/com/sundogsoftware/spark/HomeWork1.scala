package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object HomeWork1 {
  
  def parseLine(line:String) = {
    val fields = line.split(",")
    //Customer Id,    Transaction total
    (fields(0).toInt, fields(2).toFloat)
  }
 
  def main(args: Array[String]) {
    
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CountMoney")
    
    // Read each line of input data
    val lines = sc.textFile("../customer-orders.csv")
    
    // Convert to (Customer Id, Transaction total) tuples
    val parsedLines = lines.map(parseLine)
    
    // Count totals for customer
    val customerTotal = parsedLines.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey()
    
    customerTotal.map(x => (x._2, x._1)).foreach(println)
  }
}