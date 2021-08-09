/*
 * Copyright fastGQL Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.wenj91.fastgql.core.metadata;

import com.wenj91.fastgql.common.enums.DBType;
import com.wenj91.fastgql.common.enums.KeyType;
import com.wenj91.fastgql.common.types.QualifiedName;
import com.wenj91.fastgql.core.schema.Schema;
import com.wenj91.fastgql.core.schema.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to extract metadata from a database which are necessary to build {@link Schema}.
 *
 * @author Mingyi Zhang
 */
public class DBMetadata implements Metadata {

  private static final Logger log = LoggerFactory.getLogger(DBMetadata.class);

  /**
   * 数据库类型映射
   */
  private static final Map<Integer, KeyType> sqlDataTypeToKeyType = new HashMap<>();

  static {
    sqlDataTypeToKeyType.put(Types.CHAR, KeyType.STRING);
    sqlDataTypeToKeyType.put(Types.LONGVARCHAR, KeyType.STRING);
    sqlDataTypeToKeyType.put(Types.VARCHAR, KeyType.STRING);
    sqlDataTypeToKeyType.put(Types.NUMERIC, KeyType.FLOAT);
    sqlDataTypeToKeyType.put(Types.DECIMAL, KeyType.FLOAT);
    sqlDataTypeToKeyType.put(Types.BIT, KeyType.BOOL);
    sqlDataTypeToKeyType.put(Types.TINYINT, KeyType.INT);
    sqlDataTypeToKeyType.put(Types.SMALLINT, KeyType.INT);
    sqlDataTypeToKeyType.put(Types.INTEGER, KeyType.INT);
    sqlDataTypeToKeyType.put(Types.BIGINT, KeyType.INT);
    sqlDataTypeToKeyType.put(Types.REAL, KeyType.FLOAT);
    sqlDataTypeToKeyType.put(Types.FLOAT, KeyType.FLOAT);
    sqlDataTypeToKeyType.put(Types.DOUBLE, KeyType.FLOAT);
    sqlDataTypeToKeyType.put(Types.BINARY, KeyType.STRING);
    sqlDataTypeToKeyType.put(Types.VARBINARY, KeyType.STRING);
    sqlDataTypeToKeyType.put(Types.LONGVARBINARY, KeyType.STRING);
    sqlDataTypeToKeyType.put(Types.DATE, KeyType.STRING);
    sqlDataTypeToKeyType.put(Types.TIME, KeyType.STRING);
    sqlDataTypeToKeyType.put(Types.TIMESTAMP, KeyType.STRING);
  }

  /**
   * Creates {@link Schema} given {@link Connection} to the database.
   * 构建数据库及表信息
   *
   * @param option option
   * @return database schema extracted using given connection
   * @throws SQLException in case metadata cannot be extracted
   */
  @Override
  public Schema createSchema(MetadataOption option) {
    if (!(option instanceof DBMetadataOption)) {
      throw new RuntimeException("option must be DBMetadataOption");
    }

    DBMetadataOption dbMetadataOption = (DBMetadataOption) option;
    Connection connection = dbMetadataOption.getConnection();
    DBType dbType = dbMetadataOption.getDbType();
    String schemaName = dbMetadataOption.getSchemaName();

    Schema schema = new Schema(dbType);

    try {
      DatabaseMetaData databaseMetaData = connection.getMetaData();

      // 获取所有表信息
      try (Statement statement = connection.createStatement();
           ResultSet tablesResultSet =
               databaseMetaData.getTables(null, null, null, new String[]{"TABLE"})) {

        while (tablesResultSet.next()) {
          String tableCat = tablesResultSet.getString("TABLE_CAT");
          if (null != schemaName && !schemaName.equals(tableCat)) {
            continue;
          }

          String tableName = tablesResultSet.getString("TABLE_NAME");
          Map<String, String> primaryKeyMap = new HashMap<>();
          try (ResultSet primaryKeyResultSet = databaseMetaData.getPrimaryKeys(schemaName, null, tableName)) {
            while (primaryKeyResultSet.next()) {
              String columnName = primaryKeyResultSet.getString("COLUMN_NAME");
              primaryKeyMap.put(QualifiedName.generate(tableName, columnName), columnName);
            }
          }

          schema.table(tableName,
              TableInfo.builder()
                  .tableName(tableName)
                  .name(tableName)
                  .build()
          );

          // Foreign key extraction
          // 获取表外键信息
          Map<String, String> foreignKeyToRef = new HashMap<>();
          try (ResultSet foreignKeyResultSet = databaseMetaData.getImportedKeys(null, null, tableName)) {
            while (foreignKeyResultSet.next()) {
              String columnName = foreignKeyResultSet.getString("FKCOLUMN_NAME");
              String refColumnName = foreignKeyResultSet.getString("PKCOLUMN_NAME");
              String refTableName = foreignKeyResultSet.getString("PKTABLE_NAME");
              foreignKeyToRef.put(
                  QualifiedName.generate(tableName, columnName),
                  QualifiedName.generate(refTableName, refColumnName));
            }
          }

          // Column extraction
          // 获取表字段信息
          try (ResultSet columnsResultSet = databaseMetaData.getColumns(null, null, tableName, null)) {
            while (columnsResultSet.next()) {
              Integer dataType = columnsResultSet.getInt("DATA_TYPE");
              if (!sqlDataTypeToKeyType.containsKey(dataType)) {
                log.debug("Only integer, float, date or string class for columns currently supported");
                continue;
              }
              String columnName = columnsResultSet.getString("COLUMN_NAME");
              String qualifiedName = QualifiedName.generate(tableName, columnName);

              KeyType keyType = sqlDataTypeToKeyType.get(dataType);

              boolean isPrimaryKey = false;
              if (primaryKeyMap.containsKey(qualifiedName)) {
                isPrimaryKey = true;
              }

              // 如果有外键信息则添加外键信息
              if (foreignKeyToRef.containsKey(qualifiedName)) {
                schema.addKey(
                    qualifiedName,
                    qualifiedName,
                    keyType,
                    foreignKeyToRef.get(qualifiedName),
                    isPrimaryKey);
              } else {
                schema.addKey(
                    qualifiedName,
                    qualifiedName,
                    keyType, isPrimaryKey
                );
              }
            }
          }
        }
      }
      return schema;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
