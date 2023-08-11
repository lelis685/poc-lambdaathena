package example;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;


public class Handler implements RequestHandler<Map<String,String>, String> {
    private static final Logger logger = LoggerFactory.getLogger(Handler.class);
    private static final String DATE_PATTERN = "yyyyMMdd";

    @Override
    public String handleRequest(Map<String,String> event, Context context) {
        logger.info("start lambda");
        logger.info("event: {}", event);

        try {
            LocalDate dateEvent = LocalDate.parse(event.get("date"), DateTimeFormatter.ofPattern(DATE_PATTERN));
            LocalDate currentDate = LocalDate.now();
            return String.valueOf(currentDate.isEqual(dateEvent));
        } catch (Exception e) {
            throw new RuntimeException("lambda execution failed", e);
        }
    }

}
