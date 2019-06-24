package com.spark.training

import com.spark._
class Person(name:String, age:Float) {

  val prs = new persons.Person(name,age,"US")

  def display()={
    prs.display()
  }
}
