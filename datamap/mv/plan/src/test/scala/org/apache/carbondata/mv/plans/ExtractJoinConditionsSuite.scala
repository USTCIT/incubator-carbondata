/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans

import org.apache.spark.sql.catalyst.plans._
import org.apache.carbondata.mv.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

class ExtractJoinConditionsSuite extends PlanTest {
  val testRelation0 = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int)
  val testRelation2 = LocalRelation('b.int,'c.int,'e.int)
  
  test("join only") {
    val left = testRelation0.where('a === 1)
    val right = testRelation1
    val originalQuery =
      left.join(right, condition = Some("d".attr === "b".attr || "d".attr === "c".attr)).analyze
    val modularPlan = originalQuery.modularize 
    val extracted = modularPlan.extractJoinConditions(modularPlan.children(0),modularPlan.children(1))
    
    val correctAnswer = originalQuery match {
      case logical.Join(logical.Filter(cond1,logical.LocalRelation(tbl1,_)),logical.LocalRelation(tbl2,_),Inner,Some(cond2)) =>
        Seq(cond2)
    }
    
    compareExpressions(correctAnswer,extracted)
  }
  
  test("join and filter") {
    val left = testRelation0.where('b === 2).subquery('l)
    val right = testRelation2.where('b === 2).subquery('r)
    val originalQuery =
      left.join(right,condition = Some("r.b".attr === 2 && "l.c".attr === "r.c".attr)).analyze
    val modularPlan = originalQuery.modularize
    val extracted = modularPlan.extractJoinConditions(modularPlan.children(0),modularPlan.children(1))
    
    val originalQuery1 =
      left.join(right,condition = Some("l.c".attr === "r.c".attr)).analyze
      
    val correctAnswer = originalQuery1 match {
      case logical.Join(logical.Filter(cond1,logical.LocalRelation(tbl1,_)),logical.Filter(cond2,logical.LocalRelation(tbl2,_)),Inner,Some(cond3)) =>
        Seq(cond3)
    }    
    
    compareExpressions(correctAnswer,extracted)
  }
}