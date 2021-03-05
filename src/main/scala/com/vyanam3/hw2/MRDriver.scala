package com.vyanam3.hw2

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object MRDriver {

  val logger: Logger = LoggerFactory.getLogger("MRDriver.class")
  val conf: Config = ConfigFactory.load("configuration.conf")

  val inputFile: String = conf.getString("configuration.inputFile")
  val outputFile: String = conf.getString("configuration.outputFile")
  val verbose: Boolean = true
  val consecutiveYears: Int = conf.getInt("configuration.forConsecutiveYears")

  def main(args: Array[String]): Unit = {

    val startTags = conf.getString("configuration.startTags")
    val endTags = conf.getString("configuration.endTags")
//    logger.info("Deleting output directory..")
//    FileUtils.deleteDirectory(new File(outputFile));

    val configure: Configuration = new Configuration()

    //Set start and end tags for XmlInputFormat
    configure.set(MyXmlInputFormat.START_TAGS, startTags)
    configure.set(MyXmlInputFormat.END_TAGS, endTags)

    //Format as CSV output
    configure.set("mapred.textoutputformat.separator", ",")

    val job1Name = conf.getString("configuration.job1")
    val job2Name = conf.getString("configuration.job2")
    val job3Name = conf.getString("configuration.job3")
    val job4Name = conf.getString("configuration.job4")
    val job5Name = conf.getString("configuration.job5")
    val job6Name = conf.getString("configuration.job6")
    val job7Name = conf.getString("configuration.job7")

    /**
     * spreadsheet or an CSV file that shows top ten published authors at each venue
     */
    val job1: Job = Job.getInstance(configure, job1Name)
    job1.setJarByClass(classOf[Task1_TopAuthorsMapper])
    job1.setMapperClass(classOf[Task1_TopAuthorsMapper])
    job1.setReducerClass(classOf[Task1_TopAuthorsReducer])
    job1.setInputFormatClass(classOf[MyXmlInputFormat])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[Text])
    job1.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.addInputPath(job1, new Path(inputFile))
    FileOutputFormat.setOutputPath(job1, new Path((outputFile + "/" + job1Name)))


    /**
     * list of authors who published without interruption for N years where 10 <= N
     */
    val job2: Job = Job.getInstance(configure, job2Name)
    job2.setJarByClass(classOf[Task2_UninterruptedAuthorsMapper])
    job2.setMapperClass(classOf[Task2_UninterruptedAuthorsMapper])
    job2.setReducerClass(classOf[Task2_UninterruptedAuthorsReducer])
    job2.setInputFormatClass(classOf[MyXmlInputFormat])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[Text])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.addInputPath(job2, new Path(inputFile))
    FileOutputFormat.setOutputPath(job2, new Path((outputFile + "/" + job2Name)))


    /**
     * list of publications that contains only one author
     */
    val job3: Job = Job.getInstance(configure, job3Name)
    job3.setJarByClass(classOf[Task3_VenuePublicationOneAuthorMapper])
    job3.setMapperClass(classOf[Task3_VenuePublicationOneAuthorMapper])
    job3.setReducerClass(classOf[Task3_VenuePublicationOneAuthorReducer])
    job3.setInputFormatClass(classOf[MyXmlInputFormat])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[Text])
    job3.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.addInputPath(job3, new Path(inputFile))
    FileOutputFormat.setOutputPath(job3, new Path((outputFile + "/" + job3Name)))


    /**
     * list of publications for each venue that contain the highest number of authors for each of these venues
     */
    val job4: Job = Job.getInstance(configure, job4Name)
    job4.setJarByClass(classOf[Task4_VenuePublicationsHighestAuthorsMapper])
    job4.setMapperClass(classOf[Task4_VenuePublicationsHighestAuthorsMapper])
    job4.setReducerClass(classOf[Task4_VenuePublicationsHighestAuthorsReducer])
    job4.setInputFormatClass(classOf[MyXmlInputFormat])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[Text])
    job4.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.addInputPath(job4, new Path(inputFile))
    FileOutputFormat.setOutputPath(job4, new Path((outputFile + "/" + job4Name)))


    val job5: Job = Job.getInstance(configure, job5Name)
    job5.setJarByClass(classOf[Task5_Top100MostCoAuthorsMapper])
    job5.setMapperClass(classOf[Task5_Top100MostCoAuthorsMapper])
    job5.setReducerClass(classOf[Task5_Top100MostCoAuthorsReducer])
    job5.setInputFormatClass(classOf[MyXmlInputFormat])
    job5.setMapOutputKeyClass(classOf[Text])
    job5.setMapOutputValueClass(classOf[DoubleWritable])
    job5.setOutputKeyClass(classOf[Text])
    job5.setOutputFormatClass(classOf[TextOutputFormat[Text, DoubleWritable]])
    FileInputFormat.addInputPath(job5, new Path(inputFile))
    FileOutputFormat.setOutputPath(job5, new Path((outputFile + "/" + job5Name)))
    /**
     * spreadsheet or an CSV file that shows top 100 published authors at each venue
     */

    val job6: Job = Job.getInstance(configure, job6Name)
    job6.setMapperClass(classOf[Task5a_Top100AuthorsMapper])
    job6.setReducerClass(classOf[Task5a_Top100AuthorsReducer])
    job6.setOutputKeyClass(classOf[Text])
    job6.setOutputValueClass(classOf[Text])
    job6.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    job6.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job6, new Path(outputFile + "/" + job5Name))
    FileOutputFormat.setOutputPath(job6, new Path((outputFile + "/" + job6Name)))

    /**
     * spreadsheet or an CSV file that shows top ten published authors at each venue
     */
    val job7: Job = Job.getInstance(configure, job7Name)
    job7.setMapperClass(classOf[Task5b_NoCoAuthorsMapper])
    job7.setReducerClass(classOf[Task5b_NoCoAuthorsReducer])
    job7.setOutputKeyClass(classOf[Text])
    job7.setOutputValueClass(classOf[Text])
    job7.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    job7.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job7, new Path(outputFile + "/" + job5Name))
    FileOutputFormat.setOutputPath(job7, new Path((outputFile + "/" + job7Name)))


    if (job1.waitForCompletion(verbose) && job2.waitForCompletion(verbose)
      && job2.waitForCompletion(verbose) && job3.waitForCompletion(verbose)
      && job4.waitForCompletion(verbose) && job5.waitForCompletion(verbose) && job6.waitForCompletion(verbose)
      && job7.waitForCompletion(verbose))
    {
      logger.info("*** FINISHED (Execution completed in: ")
    }
  }

}
