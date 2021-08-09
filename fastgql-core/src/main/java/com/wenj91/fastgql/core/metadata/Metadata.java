package com.wenj91.fastgql.core.metadata;

import com.wenj91.fastgql.core.schema.Schema;

public interface Metadata {
  Schema createSchema(MetadataOption option) throws Exception;
}
