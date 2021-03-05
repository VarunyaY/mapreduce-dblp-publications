package com.vyanam3.hw2

import java.lang

import com.vyanam3.hw2.MRDriver.logger
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Task4_VenuePublicationsHighestAuthorsReducer extends Reducer[Text,Text,Text,Text] {
  val map:mutable.HashMap[String,Int] = new mutable.HashMap[String,Int]()

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    map.clear()
    val maxAuthorsList = scala.collection.mutable.ListBuffer.empty[(String, Int)]
    val venue:Text = key

    values.forEach(pair =>{
      val temp = (pair.toString.split("seperator"))
      maxAuthorsList.append((temp(0),temp(1).toInt))
    })

    //Among the list of tuple of author and number of co-authors, finding the maximum of the authors number
    //    and filtering the ones with this maximum number.
    val tempMax = findMaxValueInNumbersOfAuthorsList(maxAuthorsList)
    val tempMaxList = filterOutTupleBasedOnMaxValueOfNumberOfAuthors(maxAuthorsList,tempMax)

    //Concatenating the filter tuples into a text so that it can be given to the context.
    val res:Text = new Text()
    tempMaxList.foreach(pair =>{
      res.set(pair._1 + ":" + (pair._2).toString)
    })
    context.write(venue,res)
  }

  def findMaxValueInNumbersOfAuthorsList(list: scala.collection.mutable.ListBuffer[(String, Int)]): Int = {
    list.maxBy(_._2)._2
  }

  def filterOutTupleBasedOnMaxValueOfNumberOfAuthors(list: scala.collection.mutable.ListBuffer[(String, Int)], maxValue: Int): ListBuffer[(String, Int)] = {
    list.filter(_._2 == maxValue)
  }
}
