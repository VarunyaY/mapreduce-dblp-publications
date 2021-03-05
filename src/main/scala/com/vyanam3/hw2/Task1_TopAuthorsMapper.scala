package com.vyanam3.hw2

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory

//This is for Task 1 to calculate the list of top ten published authors in each venue.

class Task1_TopAuthorsMapper extends Mapper[Object, Text, Text, Text] {

  val venueText:Text = new Text
  val authorText:Text = new Text

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    //To get the dtd resource
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    //Below code is used to handle the parsing of tags and escaped entities in the XML file.
    //We encapsulate the xml input fragment into <dblp> tags and provide its document type definition(dtd) for parsing.
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //To convert to XML
    val xml = scala.xml.XML.loadString(xmlComp)

    //To look for publication
    val venue = (xml \\ "journal")
    venueText.set(venue.text)

    //To look for author tags
    val authors = (xml \\ "author")

    LoggerFactory.getLogger("MRDriver.class").debug(authors.toString())

    //Returning without any computation if there are no authors in the input element
    if (authors.isEmpty) return ;

    authors.foreach( author =>{
      authorText.set(author.text)
      context.write(venueText,authorText)
    })
  }


}
