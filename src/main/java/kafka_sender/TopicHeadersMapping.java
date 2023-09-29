// Класс для определения топика и разделения хедеров от JSON-файла.

package kafka_sender;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TopicHeadersMapping {

    // Метод для извлечения названия топика из имени JSON-файла (без расширения).
    public String getTopic(String jsonFileName) {
        String topic = jsonFileName
                .replaceFirst("[.][^.]+$", "") // Убираем расширение файла
                .replaceAll("[\\s(—]+.*$", ""); // Убираем лишние пробелы и символы после названия

        if (topic.isEmpty()) {
            throw new IllegalArgumentException("Топик с названием " + jsonFileName + " не найден");
        }

        return topic;
    }

    // Метод для обработки строки хедера, чтобы привести ее к формату "ключ значение".
    private String processHeaderLine(String line) {
        if (line.matches("^[^\\s]+\\s+[^\\s]+$")) {
            // Если строка имеет формат "ключ значение", возвращаем ее без изменений
            return line;
        } else {
            // В противном случае, заменяем все пробелы на один пробел
            return line.replaceAll("\\s+", " ");
        }
    }

    // Метод для извлечения хедеров и JSON-содержимого из файла.
    public Map<String, String> getHeadersAndJson(String jsonFileName) {
        Map<String, String> headersAndJson = new HashMap<>();
        StringBuilder jsonBuilder = new StringBuilder();
        boolean isHeadersSection = true;
        Map<String, String> headers = new HashMap<>();

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(jsonFileName), StandardCharsets.UTF_8))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (isHeadersSection) {
                    if (line.trim().isEmpty()) {
                        isHeadersSection = false;
                    } else {
                        line = processHeaderLine(line); // Обработка строки хедера
                        String[] parts = line.split("\\s+", 2); // Разделяем строку на ключ и значение
                        if (parts.length == 2) {
                            headers.put(parts[0], parts[1]);
                        }
                    }
                } else {
                    jsonBuilder.append(line).append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Создаем строку с хедерами в формате JSON
        StringBuilder headersString = new StringBuilder("{");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            headersString.append("\"").append(entry.getKey()).append("\":").append(entry.getValue()).append(",");
        }
        headersString.deleteCharAt(headersString.length() - 1); // Убираем trailing запятую
        headersString.append("}");

        // Объединяем хедеры и JSON-содержимое
        String combinedJson = headersString.toString() + jsonBuilder.toString().trim();

        headersAndJson.put("headers", headersString.toString());
        headersAndJson.put("json", jsonBuilder.toString().trim());
        headersAndJson.put("combinedJson", combinedJson);

        return headersAndJson;
    }
}
