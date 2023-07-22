package example;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import example.integration.AthenaQueryPartitionExecutor;
import example.integration.AthenaQueryReservoirExecutor;
import example.integration.SsmIntegration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class Handler implements RequestHandler<Map<String,String>, String> {
    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    private final AthenaQueryPartitionExecutor queryExecutor;
    private final AthenaQueryReservoirExecutor reservoirExecutor;
    private final SsmIntegration ssmIntegration;

    public Handler() {
        this.queryExecutor = new AthenaQueryPartitionExecutor();
        this.reservoirExecutor = new AthenaQueryReservoirExecutor();
        this.ssmIntegration = new SsmIntegration();
    }

    @Override
    public String handleRequest(Map<String,String> event, Context context) {

        logger.info("start lambda");

        try {
            queryExecutor.execute().ifPresentOrElse(
                    partitionValue -> {
                        ssmIntegration.updateParameterStore(partitionValue);
                        logger.info("update Parameter Store succeeded");
                        },
                    () -> logger.error("Query athena failed")
            );

            String reservoir = event.get("reservoir");

            logger.info("reservoir: {}", reservoir);

            reservoirExecutor.findByReservoirName(reservoir)
                    .ifPresentOrElse(System.out::println,
                    () -> System.out.println("Reservoir not found"));

        } catch (Exception e) {
            logger.error("lambda execution failed ", e);
        }

        return null;
    }

}
