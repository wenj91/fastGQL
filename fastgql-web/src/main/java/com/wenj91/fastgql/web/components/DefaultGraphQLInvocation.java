package com.wenj91.fastgql.web.components;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.Internal;
import com.wenj91.fastgql.web.ExecutionInputCustomizer;
import com.wenj91.fastgql.web.GraphQLInvocation;
import com.wenj91.fastgql.web.GraphQLInvocationData;
import org.dataloader.DataLoaderRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.WebRequest;

import java.util.concurrent.CompletableFuture;

@Component
@Internal
public class DefaultGraphQLInvocation implements GraphQLInvocation {

  @Autowired
  GraphQL graphQL;

  @Autowired(required = false)
  DataLoaderRegistry dataLoaderRegistry;

  @Autowired
  ExecutionInputCustomizer executionInputCustomizer;

  @Override
  public CompletableFuture<ExecutionResult> invoke(GraphQLInvocationData invocationData, WebRequest webRequest) {
    ExecutionInput.Builder executionInputBuilder = ExecutionInput.newExecutionInput()
        .query(invocationData.getQuery())
        .operationName(invocationData.getOperationName())
        .variables(invocationData.getVariables())
        .context(webRequest);
    if (dataLoaderRegistry != null) {
      executionInputBuilder.dataLoaderRegistry(dataLoaderRegistry);
    }
    ExecutionInput executionInput = executionInputBuilder.build();
    CompletableFuture<ExecutionInput> customizedExecutionInput = executionInputCustomizer.customizeExecutionInput(executionInput, webRequest);
    return customizedExecutionInput.thenCompose(graphQL::executeAsync);
  }

}
