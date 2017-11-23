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
package org.apache.carbondata.core.metadata.schema.table;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.TableProperty;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ParentColumnTableRelation;

/**
 * Store the information about the table.
 * it stores the fact table as well as aggregate table present in the schema
 */
public class TableInfo implements Serializable, Writable {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(TableInfo.class.getName());

  /**
   * serialization version
   */
  private static final long serialVersionUID = -5034287968314105193L;

  /**
   * name of the database;
   */
  private String databaseName;

  /**
   * table name to group fact table and aggregate table
   */
  private String tableUniqueName;

  /**
   * fact table information
   */
  private TableSchema factTable;

  /**
   * last updated time to update the table if any changes
   */
  private long lastUpdatedTime;

  /**
   * metadata file path (check if it is really required )
   */
  private String metaDataFilepath;

  /**
   * store location
   */
  private String tablePath;

  // this idenifier is a lazy field which will be created when it is used first time
  private AbsoluteTableIdentifier identifier;

  private List<DataMapSchema> dataMapSchemaList;

  private List<RelationIdentifier> parentRelationIdentifiers;

  public TableInfo() {
    dataMapSchemaList = new ArrayList<>();
  }

  /**
   * @return the factTable
   */
  public TableSchema getFactTable() {
    return factTable;
  }

  /**
   * @param factTable the factTable to set
   */
  public void setFactTable(TableSchema factTable) {
    this.factTable = factTable;
    updateParentRelationIdentifier();
  }

  private void updateParentRelationIdentifier() {
    Set<RelationIdentifier> parentRelationIdentifiers = new HashSet<>();
    this.parentRelationIdentifiers = new ArrayList<>();
    List<ColumnSchema> listOfColumns = this.factTable.getListOfColumns();
    for (ColumnSchema columnSchema : listOfColumns) {
      List<ParentColumnTableRelation> parentColumnTableRelations =
          columnSchema.getParentColumnTableRelations();
      if (null != parentColumnTableRelations) {
        for (int i = 0; i < parentColumnTableRelations.size(); i++) {
          parentRelationIdentifiers.add(parentColumnTableRelations.get(i).getRelationIdentifier());
        }
      }
    }
    this.parentRelationIdentifiers.addAll(parentRelationIdentifiers);
  }

  /**
   * @return the databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * @param databaseName the databaseName to set
   */
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * @return the tableUniqueName
   */
  public String getTableUniqueName() {
    return tableUniqueName;
  }

  /**
   * @param tableUniqueName the tableUniqueName to set
   */
  public void setTableUniqueName(String tableUniqueName) {
    this.tableUniqueName = tableUniqueName;
  }

  /**
   * @return the lastUpdatedTime
   */
  public long getLastUpdatedTime() {
    return lastUpdatedTime;
  }

  /**
   * @param lastUpdatedTime the lastUpdatedTime to set
   */
  public void setLastUpdatedTime(long lastUpdatedTime) {
    this.lastUpdatedTime = lastUpdatedTime;
  }

  /**
   * @return
   */
  public String getMetaDataFilepath() {
    return metaDataFilepath;
  }

  /**
   * @param metaDataFilepath
   */
  public void setMetaDataFilepath(String metaDataFilepath) {
    this.metaDataFilepath = metaDataFilepath;
  }

  public String getTablePath() {
    return tablePath;
  }

  public void setTablePath(String tablePath) {
    this.tablePath = tablePath;
  }

  public List<DataMapSchema> getDataMapSchemaList() {
    return dataMapSchemaList;
  }

  public void setDataMapSchemaList(List<DataMapSchema> dataMapSchemaList) {
    this.dataMapSchemaList = dataMapSchemaList;
  }

  /**
   * to generate the hash code
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((databaseName == null) ? 0 : databaseName.hashCode());
    result = prime * result + ((tableUniqueName == null) ? 0 : tableUniqueName.hashCode());
    return result;
  }

  /**
   * Overridden equals method
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TableInfo)) {
      return false;
    }
    TableInfo other = (TableInfo) obj;
    if (null == databaseName || null == other.databaseName) {
      return false;
    }

    if (null == tableUniqueName || null == other.tableUniqueName) {
      return false;
    }

    if (!tableUniqueName.equals(other.tableUniqueName)) {
      return false;
    }
    return true;
  }

  /**
   * This method will return the table size. Default table block size will be considered
   * in case not specified by the user
   */
  int getTableBlockSizeInMB() {
    String tableBlockSize = null;
    // In case of old store there will not be any map for table properties so table properties
    // will be null
    Map<String, String> tableProperties = getFactTable().getTableProperties();
    if (null != tableProperties) {
      tableBlockSize = tableProperties.get(CarbonCommonConstants.TABLE_BLOCKSIZE);
    }
    if (null == tableBlockSize) {
      tableBlockSize = CarbonCommonConstants.BLOCK_SIZE_DEFAULT_VAL;
      LOGGER.info("Table block size not specified for " + getTableUniqueName()
          + ". Therefore considering the default value "
          + CarbonCommonConstants.BLOCK_SIZE_DEFAULT_VAL + " MB");
    }
    return Integer.parseInt(tableBlockSize);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(databaseName);
    out.writeUTF(tableUniqueName);
    factTable.write(out);
    out.writeLong(lastUpdatedTime);
    out.writeUTF(metaDataFilepath);
    out.writeUTF(tablePath);
    boolean isChildSchemaExists =
        null != dataMapSchemaList && dataMapSchemaList.size() > 0;
    out.writeBoolean(isChildSchemaExists);
    if (isChildSchemaExists) {
      out.writeShort(dataMapSchemaList.size());
      for (int i = 0; i < dataMapSchemaList.size(); i++) {
        dataMapSchemaList.get(i).write(out);
      }
    }
    boolean isParentTableRelationIndentifierExists =
        null != parentRelationIdentifiers && parentRelationIdentifiers.size() > 0;
    out.writeBoolean(isParentTableRelationIndentifierExists);
    if (isParentTableRelationIndentifierExists) {
      out.writeShort(parentRelationIdentifiers.size());
      for (int i = 0; i < parentRelationIdentifiers.size(); i++) {
        parentRelationIdentifiers.get(i).write(out);
      }
    }
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.databaseName = in.readUTF();
    this.tableUniqueName = in.readUTF();
    this.factTable = new TableSchema();
    this.factTable.readFields(in);
    this.lastUpdatedTime = in.readLong();
    this.metaDataFilepath = in.readUTF();
    this.tablePath = in.readUTF();
    boolean isChildSchemaExists = in.readBoolean();
    this.dataMapSchemaList = new ArrayList<>();
    if (isChildSchemaExists) {
      short numberOfChildTable = in.readShort();
      for (int i = 0; i < numberOfChildTable; i++) {
        DataMapSchema childSchema = new DataMapSchema();
        childSchema.readFields(in);
        DataMapSchema dataMapSchema = DataMapSchemaFactory.INSTANCE
            .getDataMapSchema(childSchema.getDataMapName(), childSchema.getClassName());
        dataMapSchema.setChildSchema(childSchema.getChildSchema());
        dataMapSchema.setRelationIdentifier(childSchema.getRelationIdentifier());
        dataMapSchema.setProperties(childSchema.getProperties());
        dataMapSchemaList.add(dataMapSchema);
      }
    }
    boolean isParentTableRelationIndentifierExists = in.readBoolean();
    if (isParentTableRelationIndentifierExists) {
      short parentTableIndentifiersListSize = in.readShort();
      this.parentRelationIdentifiers = new ArrayList<>();
      for (int i = 0; i < parentTableIndentifiersListSize; i++) {
        RelationIdentifier relationIdentifier = new RelationIdentifier(null, null, null);
        relationIdentifier.readFields(in);
        this.parentRelationIdentifiers.add(relationIdentifier);
      }
    }
  }

  public AbsoluteTableIdentifier getOrCreateAbsoluteTableIdentifier() {
    if (identifier == null) {
      CarbonTableIdentifier carbontableIdentifier =
          new CarbonTableIdentifier(databaseName, factTable.getTableName(), factTable.getTableId());
      identifier = new AbsoluteTableIdentifier(tablePath, carbontableIdentifier);
    }
    return identifier;
  }

  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    this.write(new DataOutputStream(bao));
    return bao.toByteArray();
  }

  public static TableInfo deserialize(byte[] bytes) throws IOException {
    TableInfo tableInfo = new TableInfo();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    tableInfo.readFields(in);
    return tableInfo;
  }

  public List<RelationIdentifier> getParentRelationIdentifiers() {
    return parentRelationIdentifiers;
  }

  public static TableInfoBuilder builder() {
    return new TableInfo.TableInfoBuilder();
  }

  public static class TableInfoBuilder {

    // following members are required to build TableInfo

    private String databaseName;
    private String tableName;
    private List<ColumnSchema> allColumns;
    private String metaDataFilepath;
    private String tablePath;
    private List<DataMapSchema> dataMapSchemaList = new ArrayList<>();
    private Map<String, String> tableProperties;
    private BucketingInfo bucketingInfo;
    private PartitionInfo partitionInfo;

    // parent table object, which can be used during dropping of pre-aggreate table as
    // parent table will also get updated
    private CarbonTable parentTable;

    // column name -> DataMapField
    private Map<String, DataMapField> dataMapFields;

    private TableInfoBuilder() {
    }

    public TableInfo create() throws MalformedCarbonCommandException {
      if (databaseName == null || tablePath == null || metaDataFilepath == null) {
        throw new IllegalArgumentException("parameter must not be null");
      }
      TableSchema tableSchema =
          TableSchema.builder()
              .tableName(tableName)
              .allColumns(allColumns)
              .tableProperties(tableProperties)
              .partitionInfo(partitionInfo)
              .bucketingInfo(bucketingInfo)
              .create();
      TableInfo tableInfo = new TableInfo();
      tableInfo.setDatabaseName(databaseName);
      tableInfo.setTableUniqueName(databaseName + "_" + tableName);
      tableInfo.setFactTable(tableSchema);
      tableInfo.setTablePath(tablePath);
      tableInfo.setMetaDataFilepath(metaDataFilepath);
      tableInfo.setLastUpdatedTime(System.currentTimeMillis());
      tableInfo.setDataMapSchemaList(dataMapSchemaList);
      tableInfo.updateParentRelationIdentifier();
      tableInfo.getOrCreateAbsoluteTableIdentifier();
      return tableInfo;
    }

    public TableInfoBuilder databaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public TableInfoBuilder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public TableInfoBuilder tableProperties(Map<String, String> tableProperties) {
      this.tableProperties = tableProperties;
      return this;
    }

    public TableInfoBuilder schema(StructType schema) throws MalformedCarbonCommandException {
      schema.validateSchema();
      if (tableProperties == null) {
        throw new IllegalArgumentException("table property should not be null");
      }

      this.allColumns = createColumnsSchemas(
          schema,
          parentTable,
          dataMapFields);
      return this;
    }

    public TableInfoBuilder metadataFilePath(String metaDataFilepath) {
      this.metaDataFilepath = metaDataFilepath;
      return this;
    }

    public TableInfoBuilder tablePath(String tablePath) {
      this.tablePath = tablePath;
      return this;
    }

    // bucketFields must be either in sort_columns or dimension if not in sort_columns
    public TableInfoBuilder bucketFields(BucketFields bucketFields)
        throws MalformedCarbonCommandException {
      if (bucketFields != null) {
        List<ColumnSchema> bucketColumns = new ArrayList<ColumnSchema>();
        for (String bucketField : bucketFields.getBucketColumns()) {
          for (ColumnSchema column : allColumns) {
            if (column.getColumnName().equals(bucketField) &&
                column.isDimensionColumn() && !column.getDataType().isComplexType()) {
              bucketColumns.add(column);
              break;
            }
            String msg = "Bucket field must be dimension column and "
                + "not complex column, invalid bucket field: " + bucketField;
            LOGGER.error(msg);
            throw new MalformedCarbonCommandException(msg);
          }
          String msg = "Bucket field is not present in table schema: " + bucketField;
          LOGGER.error(msg);
          throw new MalformedCarbonCommandException(msg);
        }

        this.bucketingInfo = new BucketingInfo(bucketColumns, bucketFields.getNumberOfBuckets());
      }
      return this;
    }

    // partition column can be any column
    public TableInfoBuilder partitionInfo(PartitionInfo partitionInfo) {
      this.partitionInfo = partitionInfo;
      return this;
    }

    public TableInfoBuilder dataMapSchemaList(List<DataMapSchema> dataMapSchemaList) {
      this.dataMapSchemaList = dataMapSchemaList;
      return this;
    }

    public TableInfoBuilder parentTable(CarbonTable parentTable) {
      this.parentTable = parentTable;
      return this;
    }

    public TableInfoBuilder dataMapFields(Map<String, DataMapField> dataMapFields) {
      this.dataMapFields = dataMapFields;
      return this;
    }

    private List<ColumnSchema> createColumnsSchemas(
        StructType schema,
        CarbonTable parentTable,
        Map<String, DataMapField> dataMapFields
    ) throws MalformedCarbonCommandException {

      // create ColumnSchema for every field in schema
      List<ColumnSchema> allColumns = new ArrayList<>();
      List<ColumnSchema> dimensionColumns = new ArrayList<>();
      List<ColumnSchema> measureColumns = new ArrayList<>();
      List<StructField> fields = schema.getFields();
      TableProperty tableProperty = new TableProperty(schema, tableProperties);

      for (StructField field : fields) {
        List<ColumnSchema> column = field.createColumnSchema(
            tableProperty,
            parentTable,
            dataMapFields);
        if (field.isDimension(tableProperty)) {
          dimensionColumns.addAll(column);
        } else {
          measureColumns.addAll(column);
        }
      }

      allColumns.addAll(dimensionColumns);
      allColumns.addAll(measureColumns);

      // Adding dummy measure if no measure is provided.
      // TODO: remove this limitation
      if (measureColumns.size() == 0) {
        StructField dummyField = DataTypes.createStructField(
            CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE,
            DataTypes.DOUBLE);

        List<ColumnSchema> dummyColumn = dummyField.createColumnSchema(
            tableProperty,
            parentTable,
            dataMapFields);
        dummyColumn.get(0).setInvisible(true);
        allColumns.addAll(dummyColumn);
      }

//      // set whether to use InvertedIndex in column schema
//      for (ColumnSchema column : allColumns) {
//        // If the column is in sort_columns, use inverted index unless noInvertedIndex is set
//        // by user.
//        // If the column is not sort_columns, do not use inverted index always.
//        if (tableProperty.getSortColumns().contains(column.getColumnName())) {
//          if (noInvertedIndexColumns.contains(column.getColumnName())) {
//            column.setUseInvertedIndex(false);
//          } else {
//            column.setUseInvertedIndex(true);
//          }
//        } else {
//          column.setUseInvertedIndex(false);
//        }
//      }

      validateColumns(allColumns);
      return allColumns;
    }

    private void validateColumns(List<ColumnSchema> allColumns)
        throws MalformedCarbonCommandException {
      Set<String> columnIdSet = new HashSet<>();
      for (ColumnSchema columnSchema : allColumns) {
        columnIdSet.add(columnSchema.getColumnUniqueId());
      }

      if (columnIdSet.size() != allColumns.size()) {
        throw new MalformedCarbonCommandException("Two column can not have same columnId");
      }
    }

  }

}
