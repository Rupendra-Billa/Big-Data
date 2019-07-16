package com.spark.training


object Functions {

  def main(args: Array[String]): Unit = {

    var res = factorial(2)
    println("2 Factorial", res)

    res = factorial(5) // 5*4*3*2*1
    println("5 Factorial", res)


    res = higherOderFunctionForFactorial(2,calculateFactorial)
    println("2 calculateFactorial", res)

    res = higherOderFunctionForFactorial(5,calculateFactorial) // 5*4*3*2*1
    println("5 calculateFactorial", res)

    res = higherOderFunctionForFactorial(2,sumCurrentToLast) // 2+1
    println("2 sumCurrentToLast", res)

    res = higherOderFunctionForFactorial(5,sumCurrentToLast) // 5+4+3+2+1
    println("5 sumCurrentToLast", res)

    var res1 = curryingFactorial(2)(_)
    println("2 curryingFactorial", res1)
    var rs2 = res1(1)
    println("2 curryingFactorial", rs2)

    res1 = curryingFactorial(5)(_)
    println("2 curryingFactorial", res1)
    rs2 = res1(1)
    println("2 curryingFactorial", rs2)


  }


  /**
    * Nested Functions
    * A Function, with in the function is called Nested function
    * @param n
    * @return
    */
  def factorial( n:Int): Int ={

    def calFactorial( n:Int, lastVal:Int ): Int ={
      if( n <= 1) lastVal
      else calFactorial(n-1,n*lastVal)
    }

    calFactorial(n,1)
  }

  /**
    * Higher order Functions
    * A function which accepts another function as an argument is called Higher order function
    */

  def higherOderFunctionForFactorial(n:Int, f:(Int,Int) => Int) :Int={
    f(n,1)
  }

  def calculateFactorial( n:Int, lastVal:Int ): Int ={
    if( n <= 1) lastVal
    else calculateFactorial(n-1,n*lastVal)
  }

  def sumCurrentToLast( n:Int, lastVal:Int ): Int ={
    if( n <= 1) lastVal
    else sumCurrentToLast(n-1,n+lastVal)
  }


  /**
    * Currying
    * It is also a function , but we can call this function by passing partial arguments. But it will gives the
    * expected results when we pass all arguments
    */
  def curryingFactorial(n:Int)(lastValue:Int):Int={
    calculateFactorial(n,lastValue)
  }
}
