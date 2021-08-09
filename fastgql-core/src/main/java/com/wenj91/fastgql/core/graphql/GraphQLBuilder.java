package com.wenj91.fastgql.core.graphql;

import com.wenj91.fastgql.core.interceptor.DataFetcherInterceptor;
import com.wenj91.fastgql.core.metadata.MetadataOption;
import com.wenj91.fastgql.core.schema.Schema;
import com.wenj91.fastgql.core.schema.SchemaFactory;
import com.wenj91.fastgql.core.spqr.SpqrInterceptor;
import com.wenj91.fastgql.core.spqr.SpqrService;
import com.wenj91.fastgql.executor.Executor;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import io.leangen.graphql.GraphQLSchemaGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class GraphQLBuilder {
  private Executor executor;
  private List<DataFetcherInterceptor> interceptors;
  private MetadataOption option;
  private List<GraphQLSchema> graphQLSchemas;

  private GraphQLBuilder(Executor executor, MetadataOption option) {
    this.executor = executor;
    this.option = option;
    this.graphQLSchemas = new ArrayList<>();
    this.interceptors = new ArrayList<>();
  }

  public static GraphQLBuilder builder(Executor executor, MetadataOption option) {
    return new GraphQLBuilder(executor, option);
  }

  public GraphQLBuilder withInterceptors(DataFetcherInterceptor... interceptors) {
    if (interceptors != null) {
      this.interceptors.addAll(Arrays.asList(interceptors));
    }

    return this;
  }

  public GraphQLBuilder withGraphQLSchemas(GraphQLSchema... graphQLSchemas) {
    if (null != graphQLSchemas && graphQLSchemas.length > 0) {
      this.graphQLSchemas.addAll(Arrays.asList(graphQLSchemas));
    }

    return this;
  }

  public GraphQLBuilder withSPQR(String spqrBasePackages, Collection<SpqrService> services) {
    if (services == null || services.size() < 1) {
      throw new RuntimeException("services length must be > 0");
    }

    GraphQLSchemaGenerator generator = new GraphQLSchemaGenerator()
        .withBasePackages(spqrBasePackages)
        .withResolverInterceptors(new SpqrInterceptor(this.interceptors));

    for (SpqrService svc : services) {
      //register the service
      generator.withOperationsFromSingleton(svc);
    }

    this.graphQLSchemas.add(generator.generate());

    return this;
  }


  public GraphQL build() {
    Schema schema = SchemaFactory.get(option);
    schema.registerInterceptors(interceptors);

    GraphQLSchema[] graphQLSchemaArr = new GraphQLSchema[this.graphQLSchemas.size()];
    for (int i = 0; i < this.graphQLSchemas.size(); i++) {
      graphQLSchemaArr[i] = this.graphQLSchemas.get(0);
    }

    return GraphQLDefinition.create(schema, executor, graphQLSchemaArr)
        .enableQuery()
        .enableMutation()
        .build();
  }
}
