package com.wenj91.fastgql.example.config;

import com.wenj91.fastgql.common.enums.DBType;
import com.wenj91.fastgql.core.graphql.GraphQLBuilder;
import com.wenj91.fastgql.core.metadata.BeanMetadataOption;
import com.wenj91.fastgql.core.spqr.SpqrService;
import com.wenj91.fastgql.example.context.SpqrSpringContext;
import com.wenj91.fastgql.example.entity.Addresses;
import com.wenj91.fastgql.example.entity.Customers;
import com.wenj91.fastgql.example.entity.Test;
import com.wenj91.fastgql.example.entity.User;
import com.wenj91.fastgql.example.executor.JpaExecutor;
import com.wenj91.fastgql.example.interceptor.AuthorizerInterceptor;
import graphql.GraphQL;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Configuration
public class GraphQLConfig {

  private GraphQL graphQL;

  @Bean
  public GraphQL graphQL() {
    return graphQL;
  }

  @Resource
  private JpaExecutor jpaExecutor;

  @Resource
  private AuthorizerInterceptor authorizerInterceptor;

  @Resource
  private SpqrSpringContext spqrSpringContext;

  @PostConstruct
  public void init() throws Exception {
    Map<String, SpqrService> beansOfType = spqrSpringContext.getApplicationContext().getBeansOfType(SpqrService.class);
    Collection<SpqrService> collect = beansOfType.values();

    this.graphQL = GraphQLBuilder.builder(
            jpaExecutor,
            BeanMetadataOption.builder()
                .classes(List.of(
                    User.class,
                    Addresses.class,
                    Customers.class,
                    Test.class
                ))
                .dbType(DBType.mysql)
                .build()
        )
        .withSPQR("com.wenj91.fastgql.example.spqr", collect)
        .withInterceptors(authorizerInterceptor)
        .build();
  }

}
