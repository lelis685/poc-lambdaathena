package example.integration.config;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;

import java.util.ArrayList;
import java.util.Arrays;

public abstract class AthenaBaseConfig {

    protected static final String ATHENA_OUTPUT_S3_FOLDER_PATH = System.getenv("ATHENA_OUTPUT_S3_FOLDER_PATH");
    protected static final String ATHENA_DATABASE = System.getenv("ATHENA_DATABASE");
    protected static final long TIMEOUT_EXECUTION_SECONDS = Long.parseLong(System.getenv("TIMEOUT_EXECUTION_SECONDS"));
    protected static final long POLL_DELAY_SECONDS = 2;

    protected AthenaClient athena;

    public AthenaBaseConfig() {
        this.athena = buildAthenaClient();
    }

    protected String createQueryWithConditions(int limit, String... keys) {
        var whereClause = new ArrayList<String>();
        var queryBuilder = new StringBuilder("SELECT reservoir_name,subbasin,agency_name,dt FROM water wt");
        var limitClause = " LIMIT " + limit;
        Arrays.stream(keys).forEach(key -> whereClause.add(String.format("wt.%s = ?", key)));
        return keys.length < 1 ? queryBuilder + limitClause :
                queryBuilder.append(" WHERE " + String.join(" AND ", whereClause)) + limitClause;
    }


    protected QueryExecutionContext buildQueryExecutionContext() {
        return QueryExecutionContext.builder()
                .database(ATHENA_DATABASE)
                .build();
    }

    protected ResultConfiguration buildResultConfiguration() {
        return ResultConfiguration.builder()
                .outputLocation(ATHENA_OUTPUT_S3_FOLDER_PATH)
                .build();
    }

    protected boolean tryExtractExecutionStateSucceed(final String executionId) {
        final var queryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(executionId).build();
        final var executionResponse = athena.getQueryExecution(queryExecutionRequest);
        final var state = executionResponse.queryExecution()
                .status()
                .state().name();
        return state.equals(QueryExecutionState.SUCCEEDED.name());
    }


    private AthenaClient buildAthenaClient() {
        return AthenaClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }


}
