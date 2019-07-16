package com.spark.training

import com.fasterxml.jackson.databind.ObjectMapper

object ScalaClassTypes {

  def main(args: Array[String]): Unit = {

    val book1 = Book("Billa","Scala")
    val book2 = Book("Billa","Scala")

    val page1 = new Page(10,"Class Types")
    val page2 = new Page(10,"Class Types")

    if( book1 == book2 ){
      println("Book1 and Book2 are same")
    }

    if( page1 == page2 ){
      println("Page1 and Page2 are same")
    }

    val book3 = book2.copy(name="Spark")
    println(book1)
    println(book2)
    println(book3)

    val book =
      """{
        |"author":"Billa",
        |"name":"Scala"
      }""".stripMargin;

  }

}

case class Book(var author:String, var name:String)
class Page(pageNum:Int,topic:String){

  class SubPage{

  }
}
