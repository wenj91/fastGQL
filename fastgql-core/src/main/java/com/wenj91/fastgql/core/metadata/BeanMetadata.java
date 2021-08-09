package com.wenj91.fastgql.core.metadata;

import com.wenj91.fastgql.annotation.AuthGroup;
import com.wenj91.fastgql.annotation.Permissions;
import com.wenj91.fastgql.annotation.Roles;
import com.wenj91.fastgql.annotation.Referencing;
import com.wenj91.fastgql.common.enums.DBType;
import com.wenj91.fastgql.common.enums.KeyType;
import com.wenj91.fastgql.common.types.QualifiedName;
import com.wenj91.fastgql.core.schema.Schema;
import com.wenj91.fastgql.core.schema.TableInfo;
import com.wenj91.fastgql.core.util.StringUtil;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class BeanMetadata implements Metadata {
  private static final Map<Class<?>, KeyType> fieldTypeToKeyType = new HashMap<>();

  static {
    fieldTypeToKeyType.put(String.class, KeyType.STRING);
    fieldTypeToKeyType.put(Float.class, KeyType.FLOAT);
    fieldTypeToKeyType.put(float.class, KeyType.FLOAT);
    fieldTypeToKeyType.put(BigDecimal.class, KeyType.FLOAT);
    fieldTypeToKeyType.put(Boolean.class, KeyType.BOOL);
    fieldTypeToKeyType.put(boolean.class, KeyType.BOOL);
    fieldTypeToKeyType.put(Byte.class, KeyType.BOOL);
    fieldTypeToKeyType.put(byte.class, KeyType.BOOL);
    fieldTypeToKeyType.put(Long.class, KeyType.INT);
    fieldTypeToKeyType.put(long.class, KeyType.INT);
    fieldTypeToKeyType.put(Integer.class, KeyType.INT);
    fieldTypeToKeyType.put(int.class, KeyType.INT);
    fieldTypeToKeyType.put(BigInteger.class, KeyType.INT);
    fieldTypeToKeyType.put(Date.class, KeyType.STRING);
    fieldTypeToKeyType.put(java.sql.Date.class, KeyType.STRING);
    fieldTypeToKeyType.put(LocalDate.class, KeyType.STRING);
  }

  /**
   * @param option
   * @return
   */
  @Override
  public Schema createSchema(MetadataOption option) {
    if (!(option instanceof BeanMetadataOption)) {
      throw new RuntimeException("option must be BeanMetadataOption");
    }

    BeanMetadataOption beanMetadataOption = (BeanMetadataOption) option;
    DBType dbType = beanMetadataOption.getDbType();
    List<Class<?>> classes = beanMetadataOption.getClasses();

    Schema schema = new Schema(dbType);

    for (Class<?> clazz : classes) {
      Table table = clazz.getAnnotation(Table.class);
      if (null == table) {
        continue;
      }

      String name = clazz.getSimpleName();

      String tableName = name;
      if (!StringUtil.isBlank(table.name())) {
        tableName = table.name();
      }

      schema.table(name,
          TableInfo.builder()
              .tableName(tableName)
              .name(name)
              .build()
      );

      AuthGroup group = clazz.getAnnotation(AuthGroup.class);
      if (null != group) {
        final Roles[] roles = group.roles();
        if (null != roles && roles.length > 0) {
          for (Roles role : roles) {
            schema.role(AuthNaming.getNameRole(name, role.name()), role);
          }
        }

        final Permissions[] permissions = group.permissions();
        if (null != permissions && permissions.length > 0) {
          for (Permissions permission : permissions) {
            schema.permission(AuthNaming.getNamePermission(name, permission.name()), permission);
          }
        }
      }

      List<Field> allFields = getAllFields(clazz);
      for (Field field : allFields) {
        String fieldName = field.getName();
        boolean isPrimaryKey = false;

        boolean isStatic = Modifier.isStatic(field.getModifiers());
        if(isStatic) {
          continue;
        }

        Transient transientAnno = field.getAnnotation(Transient.class);
        if (null != transientAnno) {
          continue;
        }

        Id annotation = field.getAnnotation(Id.class);
        if (null != annotation) {
          isPrimaryKey = true;
        }

        String columnName = fieldName;
        Column column = field.getAnnotation(Column.class);
        if (null != column) {
          columnName = column.name();
        }

        Class<?> type = field.getType();
        KeyType keyType = fieldTypeToKeyType.get(type);
        String qualifiedName = QualifiedName.generate(name, fieldName);
        String qualifiedColumnName = QualifiedName.generate(name, columnName);

        Referencing referencing = field.getAnnotation(Referencing.class);
        if (null != referencing) {
          String refEntity = referencing.refEntity();
          String refField = referencing.refField();
          String refQualifiedName = QualifiedName.generate(refEntity, refField);

          schema.addKey(
              qualifiedName,
              qualifiedColumnName,
              keyType,
              refQualifiedName,
              isPrimaryKey
          );
        } else {
          schema.addKey(
              qualifiedName,
              qualifiedColumnName,
              keyType,
              isPrimaryKey
          );
        }
      }
    }

    return schema;
  }

  private static List<Field> getAllFields(Class<?> clazz) {
    if (clazz == null) {
      return Collections.emptyList();
    }

    List<Field> result = new ArrayList<>(getAllFields(clazz.getSuperclass()));
    List<Field> filteredFields = Arrays.stream(clazz.getDeclaredFields())
        .collect(Collectors.toList());
    result.addAll(filteredFields);
    return result;
  }
}
