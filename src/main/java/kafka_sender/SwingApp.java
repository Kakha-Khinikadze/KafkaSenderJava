// Главный класс приложения Swing для отправки сообщений в Kafka.

package kafka_sender;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.awt.datatransfer.DataFlavor;
import java.awt.dnd.DnDConstants;
import java.awt.dnd.DropTarget;
import java.awt.dnd.DropTargetAdapter;
import java.awt.dnd.DropTargetDropEvent;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import static kafka_sender.Properties.*;

public class SwingApp {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    private static String keyEntered = null;

    public static void main(String[] args) {
        SwingUtilities.invokeLater(SwingApp::createAndShowGUI);
    }

    private static void createAndShowGUI() {
        // Создание и настройка главного окна Swing.

        JFrame frame = new JFrame("Kafka Sender");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        int width = 800;
        int height = 400;
        frame.setSize(width, height);

        JPanel panel = new JPanel();
        panel.setLayout(new BorderLayout());
        panel.setBorder(new EmptyBorder(20, 20, 20, 20));

        JLabel titleLabel = new JLabel("Kafka Message Sender");
        titleLabel.setFont(new Font("Arial", Font.BOLD, 24));
        titleLabel.setHorizontalAlignment(SwingConstants.CENTER);
        panel.add(titleLabel, BorderLayout.NORTH);

        JLabel instructionLabel = new JLabel("Перетащите JSON-файл сюда:");
        instructionLabel.setFont(new Font("Arial", Font.PLAIN, 16));
        instructionLabel.setHorizontalAlignment(SwingConstants.CENTER);
        panel.add(instructionLabel, BorderLayout.NORTH);

        JTextArea textArea = new JTextArea();
        textArea.setEditable(false);
        textArea.setFont(new Font("Arial", Font.PLAIN, 14));
        JScrollPane scrollPane = new JScrollPane(textArea);
        panel.add(scrollPane, BorderLayout.CENTER);

        JPanel bottomPanel = new JPanel(new BorderLayout());

        JTextField keyTextField = new JTextField();
        keyTextField.setFont(new Font("Arial", Font.PLAIN, 14));
        keyTextField.setText("Введите ключ (опционально)");
        keyTextField.setForeground(Color.GRAY);

        // Обработчики фокуса для текстового поля ключа.
        keyTextField.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                if (keyTextField.getText().equals("Введите ключ (опционально)")) {
                    keyTextField.setText("");
                    keyTextField.setForeground(Color.BLACK);
                }
            }

            public void focusLost(java.awt.event.FocusEvent evt) {
                if (keyTextField.getText().isEmpty()) {
                    keyTextField.setText("Введите ключ (опционально)");
                    keyTextField.setForeground(Color.GRAY);
                }
            }
        });

        JPanel leftPanel = new JPanel();
        leftPanel.setLayout(new BorderLayout());
        leftPanel.add(keyTextField, BorderLayout.CENTER);
        bottomPanel.add(leftPanel, BorderLayout.CENTER);

        JButton sendButton = new JButton("Отправить");
        sendButton.setFont(new Font("Arial", Font.PLAIN, 16));
        JPanel rightPanel = new JPanel();
        rightPanel.setLayout(new BorderLayout());
        rightPanel.add(sendButton, BorderLayout.CENTER);
        bottomPanel.add(rightPanel, BorderLayout.EAST);

        panel.add(bottomPanel, BorderLayout.SOUTH);

        frame.add(panel);

        // Добавление обработчика действия для кнопки "Отправить".
        sendButton.addActionListener(actionEvent -> {
            if (keyTextField.getText().equals("Введите ключ (опционально)")) {
                keyEntered = null;
            } else {
                keyEntered = keyTextField.getText().trim();
            }
            textArea.append("Отправка записи...\n");
            sendRecord(selectedFile, textArea);
            textArea.append("-----------------------------------\n");
        });

        // Обработка перетаскивания файлов в текстовое поле.
        textArea.setDropTarget(new DropTarget(textArea, new DropTargetAdapter() {
            @Override
            public void drop(DropTargetDropEvent e) {
                try {
                    e.acceptDrop(DnDConstants.ACTION_COPY);
                    @SuppressWarnings("unchecked")
                    java.util.List<File> droppedFiles = (java.util.List<File>) e.getTransferable().getTransferData(DataFlavor.javaFileListFlavor);

                    if (!droppedFiles.isEmpty()) {
                        File selectedFile = droppedFiles.get(0);
                        String jsonFilePath = selectedFile.getAbsolutePath();
                        textArea.append("Выбранный файл: " + jsonFilePath + "\n");

                        // Сохранение выбранного файла для последующей обработки.
                        SwingApp.selectedFile = selectedFile;
                    }
                } catch (Exception ex) {
                    textArea.append("Ошибка: " + ex.getMessage() + "\n");
                    ex.printStackTrace();
                }
            }
        }));

        // Определение размеров экрана и центрирование окна.
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        int centerX = (int) (screenSize.getWidth() - frame.getWidth()) / 2;
        int centerY = (int) (screenSize.getHeight() - frame.getHeight()) / 2;
        frame.setLocation(centerX, centerY);

        frame.setVisible(true);
    }

    private static File selectedFile = null;

    private static void sendRecord(File selectedFile, JTextArea textArea) {
        // Отправка записи в Kafka на основе выбранного JSON-файла.

        if (selectedFile == null) {
            textArea.append("Ошибка: Файл не выбран\n");
            return;
        }

        // Получение топика и хедеров из JSON-файла.
        TopicHeadersMapping topicHeadersMapping = new TopicHeadersMapping();
        String jsonFileName = selectedFile.getName();
        String topic = topicHeadersMapping.getTopic(jsonFileName);
        Map<String, String> headersAndJson = topicHeadersMapping.getHeadersAndJson(selectedFile.getAbsolutePath());
        String headers = headersAndJson.get("headers");
        String jsonContent = headersAndJson.get("json");

        JksFileFinder jksFileFinder = new JksFileFinder();
        Properties producerProperties = createProducerProperties();
        jksFileFinder.findAndSetJksPaths(producerProperties);

        try (Producer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, keyEntered, jsonContent);
            if (headers != null && !headers.isEmpty()) {
                // Если есть хедеры, добавляем их в сообщение.
                JSONObject headerJson = new JSONObject(headers);
                for (String headerKey : headerJson.keySet()) {
                    String headerValue = headerJson.get(headerKey).toString();
                    Header header = new RecordHeader(headerKey, headerValue.getBytes());
                    record.headers().add(header);
                }
            }
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    textArea.append("Ошибка при отправке сообщения в Kafka: " + exception.getMessage() + "\n");
                    exception.printStackTrace();
                } else {
                    textArea.append("Сообщение успешно отправлено:\n" +
                            "Ключ: " + (keyEntered != null ? keyEntered : "null") + "\n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + dateFormat.format(new Date(metadata.timestamp())) + "\n");
                }
            });
        } catch (Exception ex) {
            textArea.append("Ошибка: " + ex.getMessage() + "\n");
            ex.printStackTrace();
        }
    }

    private static Properties createProducerProperties() {
        // Создание настроек для Kafka Producer.

        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROCKER_HOST);
        properties.put("enable.idempotence", "false");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put("security.protocol", "SSL");
        return properties;
    }

    public static class JksFileFinder {
        public void findAndSetJksPaths(Properties properties) {
            // Поиск JKS-файлов и настройка путей для SSL-аутентификации.

            File currentDirectory = new File(System.getProperty("user.dir"));
            File[] files = currentDirectory.listFiles((dir, name) -> {
                String lowerCaseName = name.toLowerCase();
                return lowerCaseName.endsWith(".jks");
            });

            String truststorePath = null;
            String keystorePath = null;

            for (File file : files) {
                String lowerCaseName = file.getName().toLowerCase();
                if (lowerCaseName.contains("trust")) {
                    truststorePath = file.getAbsolutePath();
                } else if (lowerCaseName.contains("keystore") || lowerCaseName.contains("client")) {
                    keystorePath = file.getAbsolutePath();
                }
            }

            if (truststorePath != null) {
                properties.put("ssl.truststore.location", truststorePath);
                properties.put("ssl.truststore.password", KAFKA_PASS);
            }

            if (keystorePath != null) {
                properties.put("ssl.keystore.location", keystorePath);
                properties.put("ssl.keystore.password", KAFKA_PASS);
                properties.put("ssl.key.password", KAFKA_PASS);
            }
        }
    }
}
