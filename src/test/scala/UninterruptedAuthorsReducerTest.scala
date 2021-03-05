import com.vyanam3.hw2.Task2_UninterruptedAuthorsReducer
import org.scalatest.FunSuite

import scala.::

class UninterruptedAuthorsReducerTest extends FunSuite {

  test("Tests the correctness of the sortHelper function in the UninterruptedAuthorsReducer") {
    val uninterruptedAuthorsReducer = new Task2_UninterruptedAuthorsReducer

    val list_to_be_sorted = 275 :: 300 :: 185 :: 325 :: 88 :: Nil

    val correct_list = 88 :: 185 :: 275 :: 300 :: 325 :: Nil

    val sorted_list = uninterruptedAuthorsReducer.sortHelper(list_to_be_sorted)

    assert(sorted_list == (correct_list))
    println("Test Completed")
  }

  test("Tests the correctness of the helper function that checks for uninterrupted sequence of calendar years with a list of integers which represent calendar years in UninterruptedAuthorsReducer") {
    val uninterruptedAuthorsReducer = new Task2_UninterruptedAuthorsReducer

    val yearsList = List(2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010)

    val areTheYearsOfPublicationConsecutive = uninterruptedAuthorsReducer.checkForUninterruptedYearsOfPublishing(2000,yearsList,1)

    assert(areTheYearsOfPublicationConsecutive)

    println("Test Completed")
  }
}
