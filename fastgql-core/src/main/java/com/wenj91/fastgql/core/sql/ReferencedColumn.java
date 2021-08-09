package com.wenj91.fastgql.core.sql;

import graphql.language.Field;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ReferencedColumn {
  private Field field;

  private String foreignColumnNameForReferenced;

  private SelectColumn selectColumn;
}
