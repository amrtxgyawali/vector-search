package gov.maryland.vector_search.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.ai.document.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CsvToJsonConverter {

    /**
     * Reads a CSV file and returns a list of maps where each map represents a row with column names as keys.
     *
     * @param filePath Path to the CSV file.
     * @return List of rows as maps.
     * @throws IOException If the file cannot be read.
     */
    public static List<Map<String, String>> readCsvFile(String filePath) throws IOException {
        List<Map<String, String>> rows = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IllegalArgumentException("CSV file is empty.");
            }

            // Split the header into column names
            String[] headers = headerLine.split(",");

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                Map<String, String> row = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    row.put(headers[i], i < values.length ? values[i] : null);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * Converts the CSV data read by the readCsvFile method to a JSON string.
     *
     * @param csvData List of rows from the CSV file.
     * @return JSON representation of the CSV data.
     * @throws IOException If there is an issue with JSON processing.
     */
    public static String convertCsvToJson(List<Map<String, String>> csvData) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode arrayNode = objectMapper.createArrayNode();

        for (Map<String, String> row : csvData) {
            ObjectNode jsonObject = objectMapper.createObjectNode();
            for (Map.Entry<String, String> entry : row.entrySet()) {
                jsonObject.put(entry.getKey(), entry.getValue());
            }
            arrayNode.add(jsonObject);
        }
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(arrayNode);
    }

    public static void main(String[] args) {
        String filePath = "path/to/your/csvfile.csv"; // Replace with the actual path
        try {
            // Step 1: Read CSV file
            List<Map<String, String>> csvData = readCsvFile(filePath);

            // Step 2: Convert to JSON
            String jsonOutput = convertCsvToJson(csvData);

            // Print the JSON output
            System.out.println(jsonOutput);
        } catch (IOException e) {
            System.err.println("Error processing file: " + e.getMessage());
        }
    }

    public static List<Document> createDocumentsFromFile(File file) throws IOException {
        // List to hold Document objects
        Map<String, Object> metadata = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        StringBuilder currentRecord = new StringBuilder();

        // Read the file line by line
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();  // Remove extra whitespace

                // If the line is empty, it means we reached the end of a record
                if (line.isEmpty()) {
                    if (metadata.size() > 0) {
                        // Convert the metadata to a JSON string
                        String jsonString = objectMapper.writeValueAsString(metadata);
                        // Create a Document with the JSON string content
                        currentRecord.append(jsonString).append("\n");

                        // Clear metadata map for the next record
                        metadata.clear();
                    }
                } else {
                    // Split the line into key-value pairs
                    String[] parts = line.split(":");
                    if (parts.length == 2) {
                        metadata.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }

            // Handle last record if the file does not end with a newline
            if (metadata.size() > 0) {
                String jsonString = objectMapper.writeValueAsString(metadata);
                currentRecord.append(jsonString);
            }
        }

        // Convert the string content into Document objects
        String[] jsonStrings = currentRecord.toString().split("\n");
        Document[] documents = new Document[jsonStrings.length];
        for (int i = 0; i < jsonStrings.length; i++) {
            documents[i] = new Document(jsonStrings[i]);
        }

        return List.of(documents);
    }
}

