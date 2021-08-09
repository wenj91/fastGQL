package com.wenj91.fastgql.core.metadata;

import com.wenj91.fastgql.common.enums.DBType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.sql.Connection;

@Data
@Builder
@AllArgsConstructor
public class DBMetadataOption implements MetadataOption {
  private Connection connection;
  private String schemaName;
  private DBType dbType;
}
