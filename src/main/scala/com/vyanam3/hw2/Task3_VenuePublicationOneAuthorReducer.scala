package com.vyanam3.hw2

import java.lang

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

//Task3: Reducer to find the the number of publications with only one author in each venue.
//The key indicates the venue and the value has publications
class Task3_VenuePublicationOneAuthorReducer extends Reducer[Text,Text,Text,Text] {
  val logger: Logger = LoggerFactory.getLogger("MRDriver.class")
  val map:mutable.HashMap[String,Int] = new mutable.HashMap[String,Int]()
  val res:Text = new Text

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    res.clear()
    values.forEach(value=>{
      res.set(res+","+value)
    })
//    logger.info(res.toString)
    context.write(key,res)
  }
}
