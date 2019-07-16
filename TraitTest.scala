package com.spark.training

abstract class AbSound{

  def play()

  def playing(): Unit ={
    println("Playing")
  }
}

trait Sound {

  def play()={
    println("Playing Sound")
  }
}


class Dog extends Sound{

  override def play()={
    println("Barking")
  }
}

class Cat extends Sound{

  override def play()={
    println("Miyav")
  }
}

/**
  * Mixin
  */
class Animal extends Dog with Sound{

}

object TraitTest{

  def main(args: Array[String]): Unit = {

    val sounds = new Array[Sound](3)
    sounds(0) = new Dog()
    sounds(1) = new Cat()
    sounds(2) = new Animal()

    sounds.foreach(_.play())
  }
}