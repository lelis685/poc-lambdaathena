package example.integration;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

public abstract class AthenaBaseConfig {

    protected static final String ATHENA_OUTPUT_S3_FOLDER_PATH = System.getenv("ATHENA_OUTPUT_S3_FOLDER_PATH");
    protected static final String ATHENA_DATABASE = System.getenv("ATHENA_DATABASE");
    protected static final long TIMEOUT_EXECUTION_SECONDS = Long.parseLong(System.getenv("TIMEOUT_EXECUTION_SECONDS"));
    protected static final long POLL_DELAY_SECONDS = 2;

    protected AthenaClient athena;

    public AthenaBaseConfig() {
        this.athena = buildAthenaClient();
    }

    protected QueryExecutionContext buildQueryExecutionContext(){
        return QueryExecutionContext.builder()
                .database(ATHENA_DATABASE)
                .build();
    }

    protected ResultConfiguration buildResultConfiguration(){
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
