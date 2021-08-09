package com.wenj91.fastgql.core.metadata;

public class AuthNaming {
  public static String getNamePermission(String name, String permissionPoint) {
    return String.format("%s#%s", name, permissionPoint);
  }

  public static String getNameRole(String name, String rolePoint) {
    return String.format("%s#%s", name, rolePoint);
  }
}
