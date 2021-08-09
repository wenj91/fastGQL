package com.wenj91.fastgql.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.FIELD})
public @interface AuthGroup {
  String name() default "";
  Permissions[] permissions() default {};
  Roles[] roles() default {};
}
