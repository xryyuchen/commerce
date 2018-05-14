/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 17-11-13 下午8:44.
 * Author: lenovo.
 */

package com.yuchen.sessionRequire

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  private val sessionGroupMap = mutable.HashMap[String, Int]()


  override def isZero: Boolean = {
    sessionGroupMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionStatAccumulator
    sessionGroupMap.synchronized{
      newAcc.sessionGroupMap ++= this.sessionGroupMap
    }
    newAcc
  }

  override def reset(): Unit = {
    sessionGroupMap.clear()
  }

  override def add(v: String): Unit = {
    sessionGroupMap(v) = 1 + sessionGroupMap.getOrElse(v,0)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc:SessionStatAccumulator => {
        acc.value.map{
          case(k,v) =>{
            sessionGroupMap(k) = v + sessionGroupMap.getOrElse(k,0)
          }
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.sessionGroupMap
  }
}
