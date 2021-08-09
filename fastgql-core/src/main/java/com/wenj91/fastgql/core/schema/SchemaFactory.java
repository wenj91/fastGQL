package com.wenj91.fastgql.core.schema;

import com.wenj91.fastgql.core.metadata.*;

public class SchemaFactory {
  public static Schema get(MetadataOption option) {
    if (option instanceof BeanMetadataOption) {
      return new BeanMetadata().createSchema(option);
    }

    if (option instanceof DBMetadataOption) {
        return new DBMetadata().createSchema(option);
    }

    throw new RuntimeException("only support db or bean metadata option");
  }
}
