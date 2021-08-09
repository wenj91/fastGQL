package com.wenj91.fastgql.example.spqr;

import com.wenj91.fastgql.annotation.Permissions;
import com.wenj91.fastgql.core.spqr.SpqrService;
import com.wenj91.fastgql.example.entity.SpqrUser;
import io.leangen.graphql.annotations.GraphQLArgument;
import io.leangen.graphql.annotations.GraphQLQuery;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class TestService implements SpqrService {

  @GraphQLQuery(name = "test_query")
  @Permissions(name = "test_query", value = "query")
  public SpqrUser getById(@GraphQLArgument(name = "id") Integer id) {
    return SpqrUser.builder().id(id).name("test_user").registrationDate(new Date()).build();
  }
}
