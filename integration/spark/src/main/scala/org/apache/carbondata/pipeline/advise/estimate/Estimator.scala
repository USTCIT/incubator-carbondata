package org.apache.carbondata.pipeline.advise.estimate

trait Estimator {
  /**
    * estimate the query cost of input SQL statement
    *
    * @param sql input SQL
    * @return cost
    */
  def estimate(sql: String): Int
}