package com.vyanam3.hw2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

//Task 5a: The task of the mapper is to get the top 100 authors with most number of coauthors.
class Task5a_Top100AuthorsMapper extends Mapper[Object, Text, Text, Text] {
  val numCoauthors = new DoubleWritable
  val authorKey = new Text
  val authorNumberOfCoAuthors = new Text
  val taskFor:Text = new Text

  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val logger: Logger = LoggerFactory.getLogger(getClass)


  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,Text]#Context): Unit = {

//    logger.info(key.toString)
//    logger.info(value.toString)

//    logger.info("######")
    val temp = value.toString.split(",")
    authorKey.set(temp(0))
    numCoauthors.set(temp(1).toDouble)

    //The value is created by concatenating the author key and the number of coauthors.
    authorNumberOfCoAuthors.set(authorKey + "seperator" + numCoauthors.toString)


    context.write(authorKey, authorNumberOfCoAuthors)
  }
}
