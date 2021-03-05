package com.vyanam3.hw2

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

//Task 3: A mapper to get the list of publications with only one author.
class Task3_VenuePublicationOneAuthorMapper extends Mapper[Object, Text, Text, Text] {
  val venueText: Text = new Text
  val authorText: Text = new Text
  val titleText:Text = new Text


  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    //To get the dtd resource
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    val xmlComp = s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML
    val xml = scala.xml.XML.loadString(xmlComp)

    //Look for author tags
    val venue = (xml \\ "journal")
    venueText.set(venue.text)

    //To look for author tags
    val authors = (xml \\ "author")

    val title = (xml \\ "title")
    titleText.set(title.text)

    if(authors.length==1){
      context.write(venueText, titleText)
    }
    else return
  }
}
