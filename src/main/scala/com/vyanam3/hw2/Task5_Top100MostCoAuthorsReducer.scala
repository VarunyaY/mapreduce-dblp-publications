package com.vyanam3.hw2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}


class Task5_Top100MostCoAuthorsReducer extends Reducer[Text,DoubleWritable,Text,DoubleWritable] {

  val configuration: Config = ConfigFactory.load("configuration.conf")
  val logger: Logger = LoggerFactory.getLogger(getClass)

  val value = new DoubleWritable()


  override def reduce(key: Text, values: java.lang.Iterable[DoubleWritable], context:Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context): Unit = {

    val valuesList = scala.collection.mutable.ListBuffer.empty[Double]
    //values.forEach(u => LOG.debug("author: "+key+ " elem: "+u))
    values.forEach(u => valuesList.append(u.get())) //Append to List

    //Sorted ListBuffer
    val coauthors = valuesList.sorted.reverse

    //To get the maximum of the coauthors value
    val max = coauthors(coauthors.size-1) //max value is last tuple after sorting

    //logger.info(max.toString)
    value.set(max.toDouble)
    context.write(key, value)

  }
}
