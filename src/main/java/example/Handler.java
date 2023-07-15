package example;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import example.integration.AthenaQueryExecutor;
import example.integration.SsmIntegration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Handler implements RequestHandler<ScheduledEvent, Void> {
    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    private AthenaQueryExecutor queryExecutor;
    private SsmIntegration ssmIntegration;

    @Override
    public Void handleRequest(ScheduledEvent scheduledEvent, Context context) {

        logger.info("start lambda");

        initIntegrations();

        logger.info("calling athena query executor");

        try {
            queryExecutor.execute().ifPresentOrElse(
                    partitionValue -> {
                        ssmIntegration.updateParameterStore(partitionValue);
                        logger.info("lambda execution succeeded");
                        },
                    () -> logger.error("Query athena failed")
            );

        } catch (Exception e) {
            logger.error("lambda execution failed ", e);
        }

        return null;
    }


    private void initIntegrations() {
        if (queryExecutor == null) {
            queryExecutor = buildAthenaQueryExecutor();
        }
        if (ssmIntegration == null) {
            ssmIntegration = buildSsmIntegration();
        }
    }

    private AthenaQueryExecutor buildAthenaQueryExecutor() {
        return new AthenaQueryExecutor();
    }

    private SsmIntegration buildSsmIntegration() {
        return new SsmIntegration();
    }

}
