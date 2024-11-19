package gov.maryland.vector_search.services;

import gov.maryland.vector_search.utils.CsvToJsonConverter;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.JsonMetadataGenerator;
import org.springframework.ai.reader.JsonReader;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
public class VectorServiceOld {

    @Autowired
    private VectorStore vectorStore;

    public void createAndSaveEmbeddings(String csvFilePath, String outputFilePath) throws IOException {
        // Step 1: Read CSV file from resources
        Resource csvResource = new ClassPathResource(csvFilePath.replace("classpath:", ""));
        List<Map<String, String>> csvData = CsvToJsonConverter.readCsvFile(csvResource.getFile().getAbsolutePath());

        // Step 2: Convert to JSON string
        String jsonString = CsvToJsonConverter.convertCsvToJson(csvData);

        // Step 3: Convert JSON to Documents
        Resource jsonResource = new ByteArrayResource(jsonString.getBytes(StandardCharsets.UTF_8));
        JsonReader jsonReader = new JsonReader(jsonResource, new PersonMetadataGenerator(),
                "id", "prefix", "firstname", "lastname", "fullname",
                "suffix", "ssn", "dob", "gender", "email", "phone",
                "address_Line1", "address_city", "address_state", "address_zip");

        List<Document> documents = jsonReader.get();

        // Step 4: Add to vector store
        vectorStore.add(documents);

        // Step 5: Save embeddings to file in the resource directory
        File embeddingFile = new ClassPathResource(outputFilePath.replace("classpath:", "")).getFile();
        try (FileWriter writer = new FileWriter(embeddingFile)) {
            for (Document document : documents) {
                writer.write(document.getMetadata().toString());
                writer.write(System.lineSeparator());
            }
        }
    }

    public List<Document> searchFromSavedEmbeddings(String query, String embeddingsFilePath) throws IOException {
        // Step 1: Read embeddings from the resource directory
        Resource embeddingResource = new ClassPathResource(embeddingsFilePath.replace("classpath:", ""));
        List<Document> documents = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(embeddingResource.getFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                documents.add(new Document(line));
            }
        }

        // Step 2: Add embeddings to vector store
        vectorStore.add(documents);

        // Step 3: Perform similarity search
        return vectorStore.similaritySearch(
                SearchRequest.defaults()
                        .withQuery(query)
                        .withTopK(1)
        );
    }

    private Map<String, Object> parseMetadata(String jsonString) {
        // Parse a JSON string into a Map
        return new HashMap<>(); // Implement JSON parsing (e.g., with Jackson or Gson)
    }

    public class PersonMetadataGenerator implements JsonMetadataGenerator {
        @Override
        public Map<String, Object> generate(Map<String, Object> jsonMap) {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("id", jsonMap.getOrDefault("id", ""));
            metadata.put("prefix", jsonMap.getOrDefault("prefix", ""));
            metadata.put("firstname", jsonMap.getOrDefault("firstname", ""));
            metadata.put("lastname", jsonMap.getOrDefault("lastname", ""));
            metadata.put("fullname", jsonMap.getOrDefault("fullname", ""));
            metadata.put("suffix", jsonMap.getOrDefault("suffix", ""));
            metadata.put("ssn", jsonMap.getOrDefault("ssn", ""));
            metadata.put("dob", jsonMap.getOrDefault("dob", ""));
            metadata.put("gender", jsonMap.getOrDefault("gender", ""));
            metadata.put("email", jsonMap.getOrDefault("email", ""));
            metadata.put("phone", jsonMap.getOrDefault("phone", ""));
            metadata.put("address_Line1", jsonMap.getOrDefault("address_Line1", ""));
            metadata.put("address_city", jsonMap.getOrDefault("address_city", ""));
            metadata.put("address_state", jsonMap.getOrDefault("address_state", ""));
            metadata.put("address_zip", jsonMap.getOrDefault("address_zip", ""));
            return metadata;
        }
    }
}
