package com.vyanam3.hw2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

class Task5b_NoCoAuthorsReducer  extends Reducer[Text,Text,Text,Text] {
  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val logger: Logger = LoggerFactory.getLogger(getClass)

  val value = new DoubleWritable
  val keyFinal: Text = new Text
  val maxAuthorsList = scala.collection.mutable.ListBuffer.empty[(String, Double)]
  /**
   *
   * This reducer takes tuples of the format (Author, List(num of co-authors)) and computes max, median and average statistics for each author. (Key)
   *
   * @param key Key for which tuples handled by this reducer are grouped.
   * @param values List of values of the tuples sent to this reducer with key "key".
   * @param context Output stream
   */
  override def reduce(key: Text, values: java.lang.Iterable[Text], context:Reducer[Text, Text, Text, Text]#Context): Unit = {

    values.forEach(pair =>{
      val temp = (pair.toString.split("seperator"))
      maxAuthorsList.append((temp(0),temp(1).toDouble))
    })

    sort(key,maxAuthorsList,context)
  }

  def sort(key:Text,maxAuthorsList : ListBuffer[(String, Double)],context: Reducer[Text, Text, Text, Text]#Context){
    def f(a:(String,Double),b:(String,Double)):Boolean = {
      a._2 == 1
    }
    val list2 = maxAuthorsList.sortWith(f)

//    LoggerFactory.getLogger("MRDriver.class").debug(list2.toString())
    val res:Text = new Text()
    res.set(list2.slice(0,100).toString())
//    LoggerFactory.getLogger("MRDriver.class").debug(res.toString)
    keyFinal.set("Top 100 Authors with no Co-Authors")
    context.write(keyFinal,res)
  }
}
