package com.spark.training

class Stack[A] {

  var elements:List[A] = Nil;

  def push(element:A) = elements = element :: elements

  def peek() = elements.head

  def pop() = {
    if( elements.length == 0 ){
      null.asInstanceOf[A]
    }else{
      val currentElement = peek
      elements = elements.tail
      currentElement
    }
  }

}

object GenericExample{

  def main(args: Array[String]): Unit = {

    val ints = new Stack[Int]()
    ints.push(1)
    ints.push(10)
    ints.push(20)

    println(ints.pop())
    println(ints.peek())
    println(ints.pop())

    val strings = new Stack[String]()
    strings.push("Ravi")
    strings.push("Rupen")

    println(strings.pop())
    println(strings.pop())
    println(strings.pop())


    val arr = List(1,2,3,4,5,6)
    val evenNums = for( i <- arr if i % 2 == 0 ) yield i
    println(evenNums)

    val numbers = List( List(1,5,3,7,9),List(2,4,8,10) ) // 1,5,3,7,9,2,4,8,10

    val finalValues = numbers.flatten( x => x)
    println(finalValues)

    val name = "Ravi"
    val str = name.map(x => x).mkString(",")
    println(str)






  }

}
