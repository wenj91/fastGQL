package com.wenj91.fastgql.example.entity;

import io.leangen.graphql.annotations.GraphQLQuery;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
public class SpqrUser {
  @GraphQLQuery(name = "name", description = "A person's name")
  private String name;
  @GraphQLQuery
  private Integer id;
  @GraphQLQuery(name = "regDate", description = "Date of registration")
  private Date registrationDate;
}
