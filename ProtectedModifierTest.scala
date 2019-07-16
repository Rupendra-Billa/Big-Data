package com.spark.training

class ProtectedModifierTest extends NumberTest {

  def incrment(): Unit ={
    super.incrementOneStep()
  }
}
