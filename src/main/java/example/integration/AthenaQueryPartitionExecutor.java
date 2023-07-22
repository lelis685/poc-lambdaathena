package example.integration;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class AthenaQueryPartitionExecutor extends AthenaBaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(AthenaQueryPartitionExecutor.class);

    public Optional<String> execute() {
        try {
            final var executionRequest = buildExecutionRequestPartition();
            logger.info("start request query athena execution");
            final var executionId = athena.startQueryExecution(executionRequest).queryExecutionId();
            logger.info("query executionId=" + executionId);
            await().atMost(TIMEOUT_EXECUTION_SECONDS, SECONDS)
                    .pollInterval(POLL_DELAY_SECONDS, SECONDS)
                    .until(() -> tryExtractExecutionStateSucceed(executionId));
            logger.info("athena execution query succeeded");
            return getQueryResultPartition(executionId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private StartQueryExecutionRequest buildExecutionRequestPartition() {
        final var query = """
                  SELECT dt
                  FROM "water$partitions"
                  ORDER BY dt DESC
                  LIMIT 1;
                """;
        return StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(buildQueryExecutionContext())
                .resultConfiguration(buildResultConfiguration())
                .build();
    }

    private Optional<String> getQueryResultPartition(String executionId) {
        final int numberOfLabelsNameToSkip = 1;
        logger.info("extracting result");
        final var resultSet = athena.getQueryResults(GetQueryResultsRequest.builder()
                        .queryExecutionId(executionId).build())
                .resultSet();
        return resultSet.rows().stream()
                .skip(numberOfLabelsNameToSkip)
                .flatMap(row -> row.data().stream())
                .map(Datum::varCharValue).findFirst();
    }

}
