/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.pipeline.load

import org.apache.carbondata.pipeline.common.{DictGenMapper, DictGenReducer}
import org.apache.carbondata.pipeline.{PipelineContext, Step}
import org.apache.spark.sql.SQLContext

class DictGenStep extends Step {

  override def doWork(context: PipelineContext): Unit = {

    val sqlContext = context.get("sc").asInstanceOf[SQLContext]
    val path = context.get("path").asInstanceOf[String]
    val format = context.get("format").asInstanceOf[String]
    val option = context.get("option").asInstanceOf[Map[String, String]]

    // read input file and generate dictionary
    val input = sqlContext.read
        .format(format)
        .options(option)
        .load(path)

    val mapFunc = new DictGenMapper(3)
    val writeFunc = new DictGenReducer
    input.map(mapFunc.apply)
         .reduceByKey((s1, s2)=> s1.union(s2))
         .mapValues(writeFunc.apply)
  }
}

class DictGenStepFactory extends Step.Factory {
  override def create: Step = new DictGenStep
}
