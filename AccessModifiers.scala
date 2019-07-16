package com.spark.training

/**
  * Access Specifiers :
  * 3 types
  * 1.public - Can access from anywhere
  * 2.private - Can access within the class
  * 3.protected - Only children can access
  */
object AccessModifiers extends NumberTest {

  def main(args: Array[String]): Unit = {

    incrementOneStep()

  }
}

class NumberTest{

  private var num = 0;
  protected def incrementOneStep()={
    num = num + 1;
    printValue()
  }

  private def printValue(): Unit ={
    println("num Value", num)
  }
}


