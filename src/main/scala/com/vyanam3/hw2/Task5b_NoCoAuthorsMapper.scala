package com.vyanam3.hw2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

//Task 5a: The task of the mapper is to get the top 100 authors with no coauthors.
class Task5b_NoCoAuthorsMapper extends Mapper[Object, Text, Text, Text]{
  val numCoauthors = new DoubleWritable
  val authorKey = new Text
  val authorNumberOfCoAuthors = new Text

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   *
   * This Mapper function invoked its helper function and returns tuples of the format: (Author, num of co-authors)
   * It is used as the first step of the Statistics job, in the Reducer actual statistics will then be computed on a per-author basis.
   *
   * @param key Input key -> Don't care
   * @param value Input value -> RAW XML input segment, full tag block. e.g. <article> ... </article>
   * @param context Output stream
   */
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,Text]#Context): Unit = {


    logger.info(key.toString)
    logger.info(value.toString)
    logger.info("######")
    val temp = value.toString.split(",")
    if(temp(1).toDouble == 1){
      authorKey.set(temp(0))
      numCoauthors.set(temp(1).toDouble)

      authorNumberOfCoAuthors.set(authorKey + "seperator" + numCoauthors.toString)

      context.write(authorKey, authorNumberOfCoAuthors)
    }
    else{return}

  }
}
