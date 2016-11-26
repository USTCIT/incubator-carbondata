/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.partition.api.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.scan.model.CarbonQueryPlan;
import org.apache.carbondata.spark.partition.api.DataPartitioner;
import org.apache.carbondata.spark.partition.api.Partition;

import org.apache.spark.sql.execution.command.Partitioner;

public final class QueryPartitionHelper {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(QueryPartitionHelper.class.getName());
  private static QueryPartitionHelper instance = new QueryPartitionHelper();
  private Properties properties;
  private String defaultPartitionerClass;
  private Map<String, DataPartitioner> partitionerMap =
      new HashMap<String, DataPartitioner>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  private Map<String, DefaultLoadBalancer> loadBalancerMap =
      new HashMap<String, DefaultLoadBalancer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  private QueryPartitionHelper() {

  }

  public static QueryPartitionHelper getInstance() {
    return instance;
  }

  /**
   * Read the properties from CSVFilePartitioner.properties
   */
  private static Properties loadProperties() {
    Properties properties = new Properties();

    File file = new File("DataPartitioner.properties");
    FileInputStream fis = null;
    try {
      if (file.exists()) {
        fis = new FileInputStream(file);

        properties.load(fis);
      }
    } catch (Exception e) {
      LOGGER
          .error(e, e.getMessage());
    } finally {
      if (null != fis) {
        try {
          fis.close();
        } catch (IOException e) {
          LOGGER.error(e,
              e.getMessage());
        }
      }
    }

    return properties;

  }

  /**
   * Get partitions applicable for query based on filters applied in query
   */
  public List<Partition> getPartitionsForQuery(CarbonQueryPlan queryPlan) {
    String tableUniqueName = queryPlan.getDatabaseName() + '_' + queryPlan.getTableName();

    DataPartitioner dataPartitioner = partitionerMap.get(tableUniqueName);

    List<Partition> queryPartitions = dataPartitioner.getPartitions(queryPlan);
    return queryPartitions;
  }

  public List<Partition> getAllPartitions(String databaseName, String tableName) {
    String tableUniqueName = databaseName + '_' + tableName;

    DataPartitioner dataPartitioner = partitionerMap.get(tableUniqueName);

    return dataPartitioner.getAllPartitions();
  }

  /**
   * Get the node name where the partition is assigned to.
   */
  public String getLocation(Partition partition, String databaseName, String tableName) {
    String tableUniqueName = databaseName + '_' + tableName;

    DefaultLoadBalancer loadBalancer = loadBalancerMap.get(tableUniqueName);
    return loadBalancer.getNodeForPartitions(partition);
  }
}