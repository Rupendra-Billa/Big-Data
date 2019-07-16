package com.spark.training

object Operators {

  def main(args: Array[String]): Unit = {

    ArithmeticOperator.add(10,20)
    ArithmeticOperator.subtract(20,10)
    ArithmeticOperator.multiply(2,2)
    ArithmeticOperator.divide(10,6)

    ArithmeticOperator.printResult(false)
    ArithmeticOperator.printResult(10)
    ArithmeticOperator.printResult(10.5)
    ArithmeticOperator.printResult(List(1,3,4))
  }
}

object ArithmeticOperator{

  def add(x:Int,y:Int)={
    printResult(x+y)
  }

  def subtract(x:Int,y:Int)={
    printResult(x-y)
  }

  def multiply(x:Int,y:Int)={
    printResult(x*y)
  }

  def divide(x:Int,y:Int)={
    printResult(x/y.toFloat)
  }

  def printResult(res:Any): Unit ={
    println("Result",res)
  }
}
