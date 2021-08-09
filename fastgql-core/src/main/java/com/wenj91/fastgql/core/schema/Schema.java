/*
 * Copyright fastGQL Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.wenj91.fastgql.core.schema;


import com.wenj91.fastgql.annotation.Permissions;
import com.wenj91.fastgql.annotation.Roles;
import com.wenj91.fastgql.common.enums.DBType;
import com.wenj91.fastgql.common.enums.KeyType;
import com.wenj91.fastgql.common.types.QualifiedName;
import com.wenj91.fastgql.core.interceptor.DataFetcherInterceptor;
import com.wenj91.fastgql.core.sql.Table;
import lombok.Getter;
import lombok.ToString;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Data structure defining database schema, including tables, keys, key types and foreign key
 * relationships.
 *
 * @author Kamil Bobrowski
 */
@ToString
@Getter
public class Schema {
  private final DBType dbType;

  private final Map<String, Map<String, KeyDefinition>> graph;

  private final Map<String, Set<Permissions>> permissions;
  private final Map<String, Set<Roles>> roles;
  private final Map<String, TableInfo> tables;

  private final List<DataFetcherInterceptor> interceptors = new ArrayList<>();

  public Schema(DBType dbType) {
    this.graph = new HashMap<>();
    this.dbType = dbType;
    this.permissions = new ConcurrentHashMap<>();
    this.roles = new ConcurrentHashMap<>();
    this.tables = new ConcurrentHashMap<>();
  }

  public Set<String> getTableNames() {
    return graph.keySet();
  }

  public void registerInterceptor(DataFetcherInterceptor interceptor) {
    this.interceptors.add(interceptor);
  }

  public void registerInterceptors(Collection<DataFetcherInterceptor> interceptors) {
    if (null != interceptors) {
      this.interceptors.addAll(interceptors);
    }
  }

  public synchronized Schema permissions(String name, List<Permissions> permissions) {
    if (!this.permissions.containsKey(name)) {
      this.permissions.put(name, new HashSet<>());
    }

    this.permissions.get(name).addAll(permissions);
    return this;
  }

  public synchronized Schema permission(String name, Permissions permission) {
    if (!this.permissions.containsKey(name)) {
      this.permissions.put(name, new HashSet<>());
    }

    this.permissions.get(name).add(permission);
    return this;
  }

  public synchronized Schema roles(String name, List<Roles> roles) {
    if (!this.roles.containsKey(name)) {
      this.roles.put(name, new HashSet<>());
    }

    this.roles.get(name).addAll(roles);
    return this;
  }

  public synchronized Schema table(String name, TableInfo info) {
    this.tables.put(name, info);
    return this;
  }

  public synchronized Schema role(String name, Roles role) {
    if (!this.roles.containsKey(name)) {
      this.roles.put(name, new HashSet<>());
    }

    this.roles.get(name).add(role);
    return this;
  }

  public Set<Roles> getRoles(String name) {
    return this.roles.getOrDefault(name, new HashSet<>());
  }

  public Set<Permissions> getPermissions(String name) {
    return this.permissions.getOrDefault(name, new HashSet<>());
  }

  /**
   * Add key to the table which is referencing another key.
   *
   * @param qualifiedName            qualified name of the key in a form of "table/key"
   * @param qualifiedColumnName      qualified column name of the key in a form of "table/key"
   * @param type                     type of the key
   * @param qualifiedReferencingName qualified name of a key which is referenced by this key
   * @param isPrimaryKey             column is primary
   * @return builder of DatabaseSchema
   */
  public Schema addKey(String qualifiedName, String qualifiedColumnName, KeyType type, String qualifiedReferencingName, boolean isPrimaryKey) {
    Objects.requireNonNull(qualifiedName);
    Objects.requireNonNull(qualifiedColumnName);
    Objects.requireNonNull(type);
    Objects.requireNonNull(qualifiedReferencingName);
    addKeyDefinition(
        new KeyDefinition(
            new QualifiedName(qualifiedName),
            new QualifiedName(qualifiedColumnName),
            type,
            new QualifiedName(qualifiedReferencingName),
            null)
            .primaryKey(isPrimaryKey)
    );
    return this;
  }

  /**
   * Add key to the table.
   *
   * @param qualifiedName       qualified name of the key in a form of "table/key"
   * @param qualifiedColumnName - qualified name of the key in a form of "table/key"
   * @param type                - type of the key
   * @param isPrimaryKey        column is primary
   * @return builder of DatabaseSchema
   */
  public Schema addKey(String qualifiedName, String qualifiedColumnName, KeyType type, boolean isPrimaryKey) {
    Objects.requireNonNull(qualifiedColumnName);
    Objects.requireNonNull(type);
    addKeyDefinition(new KeyDefinition(
        new QualifiedName(qualifiedName),
        new QualifiedName(qualifiedColumnName),
        type,
        null,
        null)
        .primaryKey(isPrimaryKey)
    );
    return this;
  }

  private KeyDefinition keyAt(QualifiedName qualifiedName) {
    String parent = qualifiedName.getTableName();
    String name = qualifiedName.getKeyName();
    if (graph.containsKey(parent) && graph.get(parent).containsKey(name)) {
      return graph.get(parent).get(name);
    } else {
      return null;
    }
  }

  private void mergeKeyAt(QualifiedName qualifiedName, KeyDefinition node) {
    Objects.requireNonNull(keyAt(qualifiedName)).merge(node);
  }

  private void addKeyAt(QualifiedName qualifiedName, KeyDefinition node) {
    String parent = qualifiedName.getTableName();
    String name = qualifiedName.getKeyName();
    if (!graph.containsKey(parent)) {
      graph.put(parent, new HashMap<>());
    }
    graph.get(parent).put(name, node);
  }

  private void addKeyDefinition(KeyDefinition keyDefinition) {
    QualifiedName qualifiedName = keyDefinition.getName();
    if (keyAt(qualifiedName) != null) {
      mergeKeyAt(qualifiedName, keyDefinition);
    } else {
      addKeyAt(qualifiedName, keyDefinition);
    }
    QualifiedName referencing = keyDefinition.getReferencing();
    if (referencing != null) {
      addKeyDefinition(
          new KeyDefinition(
              referencing,
              referencing,
              keyDefinition.getKeyType(),
              null,
              Stream.of(qualifiedName).collect(Collectors.toSet())));
    }
  }
}
