
package com.cebd.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCountPK {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named WordCount
    //alternative: val sc = new SparkContext("local[*]", "WordCount")
    val sc = new SparkContext(conf) 
    
    // Read each line of my book into an RDD
    val input = sc.textFile("../book.txt")
    
    // Split into words separated by a space character
   
    //val words = input.flatMap(x=> x.toLowerCase().split("\\W+"))
    //val words2 = words.flatMap(x => x.split("\\W+"))
    
    // Count up the occurrences of each word
    //val wordCounts = words.countByValue()
    
    // wordCounts sorted by flipping (word,count) to (count,word)
    //val wrdcntsorted = wordCounts.flatMap(x => (x._2,x._1)_).sortByKey()
    
    // above code was giving errors. I replicated what we did in class instead
    
    val lwrcse = input.map(x => x.toLowerCase())
    val words = lwrcse.flatMap(x => x.split("\\W+"))
   
    val WordsToRemove = List("","want","ll","make","need","some","out","you", "to", "your", "the","a", "of", "and","you","to","in","it","that","is","for","on","are","if","s","i","can","be","as","with","t","this","or","but","they","will","what","more","do","my","re","not","about","have","an","up","from","them","by","their","how","there","so","just","don","get","all","at")
    val filteredwrds = words.filter(!WordsToRemove.contains(_))
    
    val wrdcnt = filteredwrds.map(x => (x,1)).reduceByKey( (x,y) => x + y )
    
    
    val wrdcntsorted = wrdcnt.map(x => (x._2, x._1) ).sortByKey(false)
    val res=wrdcntsorted.collect().take(10)
    //println(wrdcntsorted)
    
    // Filter out all common words 
    /*val words = List("Hello", "Hello", "World")
    val badWords = List("Hello")
    val filteredWords = words.filter(!badWords.contains(_))*/
    
    
    //val filteredwrdcntsrtd = wrdcntsorted.filter(x => x._1 != "you", "to", "your", "the","a", "of", "and")
    
    
    //val res = wrdcntsorted
    
    for (result <-res) {
      val count = result._1
      val word =result._2
     println(s"$word: $count")
    }

     
  }
  
}

