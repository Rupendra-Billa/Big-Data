package com.spark.training

import com.spark.training.ClassObject.x

object Variable {

  val name = "Billa"
  def main(args: Array[String]): Unit = {

    val str = "Name"

    println(name)

    printValues("Rupen")

    ClassObject.incrementValue()
    ClassObject.incrementValue()
    ClassObject.incrementValue()

    val c1 = new Class()
    c1.incrementValue()

    val c2 = new Class()
    c2.incrementValue()

    val c3 = new Class()
    c3.incrementValue()
    c3.incrementValue()
    c3.incrementValue()

    val v2 = new Variable2()
    v2.increment(c3)

    println(ClassObject.x)
    println(c3.x)

    ClassObject.x = ClassObject.x + 2
    println( ClassObject.x)

  }

  def printValues(name:String): Unit ={
    println("Name value in printAddress method", name)
    println("Name value in printAddress method", this.name)
  }

}

object ClassObject{

  var x = 10
  def incrementValue(): Unit ={
    x = x +1;
  }

}

class Class{
  var x = 10
  def incrementValue(): Unit ={
    x = x +1;
  }
}

