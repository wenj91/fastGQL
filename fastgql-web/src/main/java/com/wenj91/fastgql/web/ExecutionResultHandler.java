package com.wenj91.fastgql.web;

import graphql.ExecutionResult;
import graphql.PublicSpi;

import java.util.concurrent.CompletableFuture;

@PublicSpi
public interface ExecutionResultHandler {

    Object handleExecutionResult(CompletableFuture<ExecutionResult> executionResultCF);
}
