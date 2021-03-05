package com.vyanam3.hw2

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.join.TupleWritable
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

//Task4: This file contains the mapper to find teh list of publications for each venue that has highest
//number of authors.
class Task4_VenuePublicationsHighestAuthorsMapper extends Mapper[Object, Text, Text, Text] {
  val venueText:Text = new Text
  val authorText:Text = new Text
  val titleText:Text = new Text
  var titleAuthor: Text = new Text
  val logger: Logger = LoggerFactory.getLogger("Driver.class")

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    //Get dtd resource on filesystem
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML
    val xml = scala.xml.XML.loadString(xmlComp)

    //Look for author tags
    val venue = (xml \\ "journal")
    venueText.set(venue.text)

    val authors = (xml \\ "author")
//    logger.info(authors.toString)

    val title = (xml \\ "title")
    titleText.set(title.text)

    titleAuthor.set(titleText + "seperator" + (authors.size).toString)

//    logger.debug(authors.toString)
    if (authors.isEmpty) return ;

    context.write(venueText,titleAuthor)
  }
}