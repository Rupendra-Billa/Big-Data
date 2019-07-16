package com.spark.training

class CompanionObject {

  private var x = 100;
  private def add(): Unit ={
    x = x+100
  }
}

object CompanionObject{

  val co = new CompanionObject()

  def main(args: Array[String]): Unit = {

    println(co.x)
    co.add()
    println(co.x)
  }
}
