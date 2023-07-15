package example.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.PutParameterRequest;

public class SsmIntegration {
    private static final Logger logger = LoggerFactory.getLogger(SsmIntegration.class);
    private static final String PARAMETER_STORE_NAME = System.getenv("PARAMETER_STORE_NAME");

    private SsmClient ssm;

    public void updateParameterStore(String value) {
        init();

        logger.info("start request Parameter Store ");

        PutParameterRequest parameterRequest = PutParameterRequest
                .builder()
                .name(PARAMETER_STORE_NAME)
                .value(value)
                .type("String")
                .overwrite(true)
                .build();

        ssm.putParameter(parameterRequest);

        logger.info("Parameter Store updated with value {}", value);
    }


    private void init() {
        if (ssm == null) {
            ssm = buildSsmClient();
        }
    }

    private SsmClient buildSsmClient() {
        return SsmClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }

}
