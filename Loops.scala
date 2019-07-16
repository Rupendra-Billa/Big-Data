package com.spark.training

import org.apache.commons.lang.ArrayUtils

/**
  * while
  * do while
  * for
  * for each
  * map
  */
object Loops {

  def main(args: Array[String]): Unit = {

    var i = -1
   while( i <= 10 ){
     println("While",i)
     if( i == -1 ) i = 5
     i +=1
   }

    i = -1;

    do{
      if( i == -1 ) i = 5
      println("Do-While",i)
      i +=1
    }while( i <= 10)


    val arr = new Array[Int](10)
    println(arr.toList)
    for( i <- 0 until arr.length ){
      arr(i) = i
    }

    println(arr.toList)

    for( num <- arr ){
      println(num)
    }




  }
}
