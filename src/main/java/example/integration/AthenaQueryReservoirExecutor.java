package example.integration;


import example.integration.dto.ReservoirDetailsDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class AthenaQueryReservoirExecutor extends AthenaBaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(AthenaQueryReservoirExecutor.class);

    public Optional<List<ReservoirDetailsDto>> findByReservoirName(String reservoirName) {
        try {
            final var executionRequest = buildExecutionRequest(reservoirName);

            logger.info("start request query athena execution");

            final var executionId = athena.startQueryExecution(executionRequest).queryExecutionId();

            logger.info("query executionId=" + executionId);

            await().atMost(TIMEOUT_EXECUTION_SECONDS, SECONDS)
                    .pollInterval(POLL_DELAY_SECONDS, SECONDS)
                    .until(() -> tryExtractExecutionStateSucceed(executionId));

            logger.info("athena execution query reservoir succeeded");
            List<ReservoirDetailsDto> result = getQueryResult(executionId);

            return result.isEmpty() ?
                    Optional.empty() :
                    Optional.of(result);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private StartQueryExecutionRequest buildExecutionRequest(String reservoirName) {
        String query = createQueryWithConditions(3, "reservoir_name");
        return StartQueryExecutionRequest.builder()
                .queryString(query)
                .executionParameters( "\"" + reservoirName + "\"")
                .queryExecutionContext(buildQueryExecutionContext())
                .resultConfiguration(buildResultConfiguration())
                .build();
    }

    private List<ReservoirDetailsDto> getQueryResult(String executionId) {
        final int numberOfLabelsNameToSkip = 1;
        logger.info("extracting result");
        final var resultSet = athena.getQueryResults(GetQueryResultsRequest.builder()
                        .queryExecutionId(executionId).build())
                .resultSet();
        final var result = new ArrayList<ReservoirDetailsDto>();
        resultSet.rows().stream()
                .skip(numberOfLabelsNameToSkip)
                .forEach(row -> {
                    var data = row.data();
                    var reservoirDetails = new ReservoirDetailsDto(
                            data.get(0).varCharValue(),
                            data.get(1).varCharValue(),
                            data.get(2).varCharValue(),
                            data.get(3).varCharValue());
                    result.add(reservoirDetails);
                });

        return result;
    }

}
