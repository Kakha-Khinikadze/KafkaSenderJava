// Класс для хранения конфигурационных свойств приложения.

package kafka_sender;

public class Properties {
    public static final String BROCKER_HOST = System.getenv("BROCKER_HOST");
    public static final String KAFKA_PASS = System.getenv("KAFKA_PASS");
}
