package example.integration;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class AthenaQueryExecutor {
    private static final Logger logger = LoggerFactory.getLogger(AthenaQueryExecutor.class);

    private static final String ATHENA_OUTPUT_S3_FOLDER_PATH = "s3://lelis-result-athena/result-lambda";
    private static final String ATHENA_DATABASE = "water_data_database";

    private static final String QUERY =
            """
                      SELECT dt
                      FROM "water$partitions"
                      ORDER BY dt DESC
                      LIMIT 1;
                    """;

    private AthenaClient athena;

    public Optional<String> execute() {

        try {
            init();

            final var executionRequest = buildExecutionRequest();

            logger.info("start request");

            final var executionId = athena.startQueryExecution(executionRequest).queryExecutionId();

            logger.info("query executionId=" + executionId);

            await()
                    .atMost(30, SECONDS)
                    .pollInterval(2, SECONDS)
                    .until(() -> tryExtractExecutionStateSucceed(executionId));

            logger.info("athena execution query succeeded");

            return getQueryResult(executionId);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<String> getQueryResult(String executionId) {
        final int numberOfLabelsNameToSkip = 1;
        logger.info("extracting result");
        final var resultSet = athena.getQueryResults(
                GetQueryResultsRequest.builder()
                        .queryExecutionId(executionId)
                        .build()).resultSet();
        return resultSet.rows().stream()
                .skip(numberOfLabelsNameToSkip)
                .flatMap(row -> row.data().stream())
                .map(Datum::varCharValue)
                .findFirst();
    }

    private static StartQueryExecutionRequest buildExecutionRequest() {
        final var queryExecutionContext = QueryExecutionContext.builder()
                .database(ATHENA_DATABASE)
                .build();
        final var resultConfiguration = ResultConfiguration.builder()
                .outputLocation(ATHENA_OUTPUT_S3_FOLDER_PATH)
                .build();
        return StartQueryExecutionRequest.builder()
                .queryString(QUERY)
                .queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration)
                .build();
    }

    private boolean tryExtractExecutionStateSucceed(final String executionId) {
        final var queryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(executionId).build();
        final var executionResponse = athena.getQueryExecution(queryExecutionRequest);
        final var state = executionResponse.queryExecution()
                .status()
                .state().name();
        logger.info("checking state " + state);
        return state.equals(QueryExecutionState.SUCCEEDED.name());
    }

    private void init() {
        if (athena == null) {
            athena = buildAthenaClient();
        }
    }

    private AthenaClient buildAthenaClient() {
        return AthenaClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }


}
