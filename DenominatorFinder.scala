package com.spark.training

import org.apache.commons.lang3.math.NumberUtils

//Code to find the greatest common denominator of two numbers

object DenominatorFinder {

  def main(args: Array[String]): Unit = {

    if(args.length != 2 ) throw new Error("Should pass 2 arguments of type Integer")

    val num1 = NumberUtils.toInt(args(0))
    val num2 = NumberUtils.toInt(args(1))

    val gdnmtr = findGreatestDenominator(num1,num2)
    println(s"Greatest denominator of $num1 and $num2 is $gdnmtr")
  }

  def findGreatestDenominator(num1:Int, num2:Int)={

    val minNum = Math.min(num1,num2)

    var canLoop = true;
    var dnmtr = minNum.toDouble;
    while( canLoop ){

      val res1 = num1 % dnmtr
      val res2 = num2 % dnmtr
      if( res1 == 0 && res2 == 0 ) canLoop = false;
      else dnmtr = dnmtr - 1
    }

    dnmtr.toInt
  }
}
