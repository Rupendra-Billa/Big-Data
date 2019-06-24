package com.spark.training

import com.spark.persons

object Testing {

  val x = 10;
  var y = 10;

  def add() = {
    println(20)
    println(30)
    println(40)
  }

  def main( x:Int)={
    print("Value : "+x)
  }

  def main(args: Array[String]): Unit = {

    println("Testing")

    val prs = new Person("Rupen",30);
    prs.display()

    val prs1 = new persons.Person("Ryan",30,"Newyork")
    prs1.display()
    add()
  }


  def findMax( table:String, col:String)={

  }
}
