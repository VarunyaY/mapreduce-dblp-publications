package com.vyanam3.hw2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

//Task5: The function of this mapper is to get the input for Job6 and Job7. The mapper and reducer
//would generate a list of tuples with author and number of coauthors.

class Task5_Top100MostCoAuthorsMapper extends Mapper[Object, Text, Text, DoubleWritable] {
  val numCoauthors = new DoubleWritable
  val authorKey = new Text

  //Initialize Config and Logger objects from 3rd party libraries
  val configuration: Config = ConfigFactory.load("configuration.conf")
  val logger: Logger = LoggerFactory.getLogger("MRDriver.class")

  /**
   *
   * This helper function takes the author name as a String and the number of co-authors for that specific author and emits the related tuple.
   *
   * @param author Name of Author as String
   * @param num Number of co-authors for author in first parameter
   * @param context
   */
  private def getNumOfCoAuthors(author: String, num: Double, context:Mapper[Object,Text,Text,DoubleWritable]#Context): Unit = {

    authorKey.set(author)
    numCoauthors.set(num)

    //To write output tuple (e.g. ("author", 3))
    //logger.debug("AuthorStatisticsMapper - OUTPUT: ("+author+", "+num+")")
    context.write(authorKey, numCoauthors)
  }

  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,DoubleWritable]#Context): Unit = {

    //To get the dtd resource
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML
    val xml = scala.xml.XML.loadString(xmlComp)

    //Look for author tags
    val authors = (xml \\ "author")

    //logger.info("----------- Authors: " + authors.toString())

    //Returning without any computation if there are no authors in the input element
    if (authors.isEmpty) {
      return
    }

    //For each author -> insert a tuple
    authors.foreach(author => getNumOfCoAuthors(author.text, authors.size, context))
  }
}
