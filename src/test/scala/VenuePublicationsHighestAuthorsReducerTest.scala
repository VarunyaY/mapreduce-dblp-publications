import com.vyanam3.hw2.Task4_VenuePublicationsHighestAuthorsReducer
import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

class VenuePublicationsHighestAuthorsReducerTest extends FunSuite{

  test("Test the correctness of findMaxValueInNumbersOfAuthorsList function in VenuePublicationsHighestAuthorsReducer") {
    val venuePublicationsHighestAuthorsReducer = new Task4_VenuePublicationsHighestAuthorsReducer

    val testList = new ListBuffer[(String, Int)]

    testList.addOne("Inheritance for ADTs",10)
    testList.addOne("Cloud Computing",5)
    testList.addOne("Software Engineering",42)
    testList.addOne("Distributed Computing",11)
    testList.addOne("Compiler Theory",23)

    val maxNumberOfAuthors = venuePublicationsHighestAuthorsReducer.findMaxValueInNumbersOfAuthorsList(testList)

    assert(maxNumberOfAuthors==42)
    println("Test Completed")

  }

  test("Test the correctness of filterOutTupleBasedOnMaxValueOfNumberOfAuthors function in VenuePublicationsHighestAuthorsReducer") {
    val venuePublicationsHighestAuthorsReducer = new Task4_VenuePublicationsHighestAuthorsReducer

    val testList = new ListBuffer[(String, Int)]

    testList.addOne("Inheritance for ADTs",10)
    testList.addOne("Cloud Computing",5)
    testList.addOne("Software Engineering",42)
    testList.addOne("Distributed Computing",11)
    testList.addOne("Compiler Theory",23)

    val maxNumberOfAuthors = venuePublicationsHighestAuthorsReducer.findMaxValueInNumbersOfAuthorsList(testList)

    val publicationWithMaxAuthors = venuePublicationsHighestAuthorsReducer.filterOutTupleBasedOnMaxValueOfNumberOfAuthors(testList,maxNumberOfAuthors)

    assert(publicationWithMaxAuthors(0)._1 == "Software Engineering")
    println("Test Completed")
  }
}
