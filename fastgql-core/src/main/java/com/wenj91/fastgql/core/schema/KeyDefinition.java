/*
 * Copyright fastGQL Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.wenj91.fastgql.core.schema;


import com.wenj91.fastgql.common.enums.KeyType;
import com.wenj91.fastgql.common.types.QualifiedName;
import lombok.Data;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Definition of a single key in a table, intended to be use as part of {@link Schema}.
 *
 * @author Kamil Bobrowski
 */
@Data
public class KeyDefinition {
  /**
   * 字段全路径：table/field
   */
  private final QualifiedName qualifiedName;
  /**
   * 字段全路径：table/bean-field
   */
  private final QualifiedName name;
  /**
   * 字段类型： INT, FLOAT, STRING, BOOL
   */
  private final KeyType keyType;
  /**
   * 如果字段存在外键，则此属性为外键信息：table(外键引用表)/key(外键字段)
   */
  private QualifiedName referencing;
  /**
   * 如果字段被其他表外键引用，则此属性为引用的外键信息：table(引用该字段的其他表)/key(引用该字段的其他表的外键字段)
   */
  private final Set<QualifiedName> referencedBy;
  /**
   * 是否主键
   */
  private boolean isPrimaryKey;

  /**
   * Define table key.
   *
   * @param qualifiedName   qualified name which specifies which key of which table is being defined
   * @param keyType         type of the key
   * @param referencing     which key is being referenced by this key
   * @param referencedBySet set of keys which are referencing this key
   */
  public KeyDefinition(
      QualifiedName name,
      QualifiedName qualifiedName,
      KeyType keyType,
      QualifiedName referencing,
      Set<QualifiedName> referencedBySet) {
    Objects.requireNonNull(qualifiedName);
    Objects.requireNonNull(keyType);
    this.isPrimaryKey = false;
    this.qualifiedName = qualifiedName;
    this.name = name;
    this.keyType = keyType;
    this.referencing = referencing;
    this.referencedBy = Objects.requireNonNullElseGet(referencedBySet, HashSet::new);
  }

  public KeyDefinition primaryKey(boolean isPrimaryKey) {
    this.isPrimaryKey = isPrimaryKey;
    return this;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public void addReferredBy(QualifiedName name) {
    this.referencedBy.add(name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KeyDefinition otherNode = (KeyDefinition) o;
    return Objects.equals(qualifiedName, otherNode.qualifiedName)
        && Objects.equals(name, otherNode.name)
        && Objects.equals(keyType, otherNode.keyType)
        && Objects.equals(referencing, otherNode.referencing)
        && Objects.equals(referencedBy, otherNode.referencedBy);
  }

  /**
   * Merge information from other key to this key. If two keys are compatible (the same qualified
   * name, key type and another key being referenced) it will add all keys which are referencing
   * another key to this key.
   *
   * @param otherKeyDefinition - other key to be merged with this key
   */
  public void merge(KeyDefinition otherKeyDefinition) {
    if (this == otherKeyDefinition) {
      return;
    }
    if (otherKeyDefinition == null) {
      throw new RuntimeException("cannot merge with null key");
    }
    if (!(Objects.equals(qualifiedName, otherKeyDefinition.qualifiedName)
        && Objects.equals(keyType, otherKeyDefinition.keyType))) {
      throw new RuntimeException("keys to be merged not compatible");
    }
    if (referencing != null
        && otherKeyDefinition.referencing != null
        && !referencing.equals(otherKeyDefinition.referencing)) {
      throw new RuntimeException("key is already referencing other key");
    }
    if (otherKeyDefinition.referencing != null) {
      this.referencing = otherKeyDefinition.referencing;
    }
    if (otherKeyDefinition.referencedBy == null) {
      return;
    }
    for (QualifiedName qualifiedName : otherKeyDefinition.referencedBy) {
      addReferredBy(qualifiedName);
    }
  }
}
