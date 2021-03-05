package com.vyanam3.hw2

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

//Task 2: A mapper to find the list of authors who published without interruption for N years. The N is assigned
//in the configuration file.
class Task2_UninterruptedAuthorsMapper extends Mapper[Object, Text, Text, Text] {

  val yearText:Text = new Text
  val authorText:Text = new Text

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    //To get the dtd resource
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML
    val xml = scala.xml.XML.loadString(xmlComp)

    val year = (xml \\ "year")
    if (year.isEmpty) return

    yearText.set(year.text)

    //To look for author tags
    val authors = (xml \\ "author")

    //Returning without any computation if there are no authors in the input element
    if (authors.isEmpty) return

    authors.foreach(author=>{
      authorText.set(author.text)
      context.write(authorText,yearText)
    })
  }

}
