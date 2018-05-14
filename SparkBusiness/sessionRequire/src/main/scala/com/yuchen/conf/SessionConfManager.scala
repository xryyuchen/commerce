/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 17-11-13 下午7:11.
 * Author: lenovo.
 */

package com.yuchen.conf

import java.io.FileInputStream
import java.util.Properties

import net.sf.json.JSONObject

object SessionConfManager {

  private val jdbcProp = loadProperties("session.properties")
  val jdbcURL = jdbcProp.getProperty("jdbc.url")
  val jdbcUSR = jdbcProp.getProperty("jdbc.user")
  val jdbcPW = jdbcProp.getProperty("jdbc.password")
  val jdbcDataSourceSize  = jdbcProp.getProperty("jdbc.datasource.size")
  val locaPath = jdbcProp.getProperty("localPath")
  private  val taskParamsJSON = jdbcProp.getProperty("task.params.json")

  val taskParams: JsonParamAnalyze = new JsonParamAnalyze(taskParamsJSON)

  def loadProperties(file:String):  Properties ={
    val prop = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource(file).getPath
    prop.load(new FileInputStream(path))
    prop
  }

  class JsonParamAnalyze(json:String){
    private val jsonObj = JSONObject.fromObject(json)

    def getParam(field:String): String ={
      jsonObj.getString(field)
    }
  }


}


