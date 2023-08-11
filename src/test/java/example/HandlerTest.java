package example;

import com.amazonaws.services.lambda.runtime.Context;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.utils.ImmutableMap;



import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class HandlerTest {

    @Mock
    private Context context;

    @InjectMocks
    private Handler handler;

    @Test
    @DisplayName("Deve retornar true para data parametro de hoje valida")
    void shouldReturnTrueWhenValidDate() {
        var key = "date";
        var value = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Map<String, String> event = ImmutableMap.of(key, value);

        String actual = handler.handleRequest(event, context);
        String expect = "true";

        assertEquals(expect, actual);
    }

    @Test
    @DisplayName("Deve retornar false para data parametro dia anterior")
    void shouldReturnFalseWhenInValidDate() {
        var key = "date";
        var value = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Map<String, String> event = ImmutableMap.of(key, value);

        String actual = handler.handleRequest(event, context);
        String expect = "false";

        assertEquals(expect, actual);
    }

    @Test
    @DisplayName("Deve estourar exception quando data formato invalido")
    void shouldReturnThrowsExceptionWhenInValidDate() {
        var key = "date";
        var value = "2025555";
        Map<String, String> event = ImmutableMap.of(key, value);

        Exception exception = assertThrows(RuntimeException.class, () -> {
            handler.handleRequest(event, context);
        });

        String expectedMessage = "lambda execution failed";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

}