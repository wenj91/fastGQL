package com.wenj91.fastgql.core.metadata;

import com.wenj91.fastgql.common.enums.DBType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
public class BeanMetadataOption implements MetadataOption {
  private List<Class<?>> classes;
  private DBType dbType;
}
