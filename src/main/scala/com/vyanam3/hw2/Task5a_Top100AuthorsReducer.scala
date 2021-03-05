package com.vyanam3.hw2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Sorting

class Task5a_Top100AuthorsReducer extends Reducer[Text,Text,Text,Text]  {

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val logger: Logger = LoggerFactory.getLogger(getClass)

  val value = new DoubleWritable
  val keyFinal: Text = new Text
  val maxAuthorsList = scala.collection.mutable.ListBuffer.empty[(String, Double)]

  override def reduce(key: Text, values: java.lang.Iterable[Text], context:Reducer[Text, Text, Text, Text]#Context): Unit = {
    values.forEach(pair =>{
      val temp = (pair.toString.split("seperator"))
      maxAuthorsList.append((temp(0),temp(1).toDouble))
    })
    sort(key,maxAuthorsList,context)
  }

  def sort(key:Text,maxAuthorsList : ListBuffer[(String, Double)],context: Reducer[Text, Text, Text, Text]#Context){
    def f(a:(String,Double),b:(String,Double)):Boolean = {
      a._2 > b._2
    }

    val list2 = maxAuthorsList.sortWith(f)

    //LoggerFactory.getLogger("MRDriver.class").debug(list2.toString())
    val res:Text = new Text()
    res.set(list2.slice(0,100).toString())
    //LoggerFactory.getLogger("MRDriver.class").debug(res.toString)
    keyFinal.set("Top 100 Authors")
    context.write(keyFinal,res)
  }
}
