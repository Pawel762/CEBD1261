package com.cebd.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the average by first name. */
object FriendsByAgebucketex {
  
  /** A function that splits a line of input into (Name, age) tuples. */
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the name and age fields, and convert age to integers
      val name = fields(1)
      val age = fields(2).toInt
      // Create a tuple that is our result.
      (name, age)
  }
  
 
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    val conf = new  SparkConf().setMaster("local[*]").setAppName("FriendsByAge").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named FriendsByAge
    // alternative: val sc = new SparkContext("local[*]", "FriendsByAge")
    val sc = new SparkContext(conf)
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../fakefriends.csv")
    
    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
    val totalsByname = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    
    // (PK) Trying to create buckets where age is less than bucket threshold
    
    // If / else syntax
  if (age > 30) println("Impossible!") else println("The world makes sense.")
    
    
    // So now we have tuples of (name, (totalage, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByName = totalsByname.mapValues(x => x._1 / x._2)
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = averagesByName.collect()
    
    // Sort and print the final results.
    results.sorted.foreach(println)
  }
    
}
  