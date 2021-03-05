package com.vyanam3.hw2

import java.lang

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.mutable

class Task2_UninterruptedAuthorsReducer extends Reducer[Text,Text,Text,Text]{
  val conf: Config = ConfigFactory.load("configuration.conf")
  val hashset:mutable.HashSet[Int] = new mutable.HashSet[Int]
  val years:Int = conf.getString("configuration.forConsecutiveYears").toInt
  val authorText:Text = new Text()
  val logger: Logger = LoggerFactory.getLogger("MRDriver.class")

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

    hashset.clear()
    values.forEach(value=>{
      hashset.add(value.toString.toInt)
    })
    val list:List[Int] = hashset.toList

    val listYears: List[Int] = sortHelper(list)
//    logger.debug(list.size.toString)
    if ( listYears.size >= years && checkForUninterruptedYearsOfPublishing(listYears.head,listYears.tail,1))
      context.write(key,new Text())
  }

  def sortHelper(list: List[Int]): List[Int] = list.sortWith((a, b) => {
    a < b
  })

  @tailrec
  final def checkForUninterruptedYearsOfPublishing(pre:Int, list:List[Int], count:Int):Boolean = {

    if (list.isEmpty || count == years)
      if (count == years) return true
      else return false

    if(list.head.equals(pre+1)) {
      checkForUninterruptedYearsOfPublishing(list.head,list.tail,count+1)
    } else {
      checkForUninterruptedYearsOfPublishing(list.head,list.tail,1)
    }
  }

}
