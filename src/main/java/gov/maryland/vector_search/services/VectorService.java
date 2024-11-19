package gov.maryland.vector_search.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.maryland.vector_search.utils.CsvToJsonConverter;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.EmbeddingClient;
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
public class VectorService {

    @Autowired
    private VectorStore vectorStore;

    @Autowired
    private EmbeddingClient embeddingClient;

    private static List<Document> documentsInStore;

    /**
     * Generate and save embeddings from a CSV file.
     *
     * @param csvFilePath    Path to the input CSV file.
     * @param metadataPath Path to save the generated embeddings.
     * @throws IOException If any file operation fails.
     */
    public void createAndSaveEmbeddings(String csvFilePath, String metadataPath, String embeddingPath) throws IOException {
        // Step 1: Load CSV file from resources
        Resource csvResource = new ClassPathResource(csvFilePath.replace("classpath:", ""));
        List<Map<String, String>> csvData = CsvToJsonConverter.readCsvFile(csvResource.getFile().getAbsolutePath());

        // Step 2: Convert CSV data to JSON
        String jsonString = CsvToJsonConverter.convertCsvToJson(csvData);

        // Step 3: Read JSON into Documents
        Resource jsonResource = new ByteArrayResource(jsonString.getBytes(StandardCharsets.UTF_8));
        JsonReader jsonReader = new JsonReader(jsonResource, new PersonMetadataGenerator(),
                "id", "prefix", "firstname", "lastname", "fullname", "email", "phone");
        List<Document> documents = jsonReader.get();

        Map<String, List<Double>> embeddings = new HashMap<>();
        // Step 4: Generate embeddings and add to documents
        for (Document document : documents) {
            String textToEmbed = document.getMetadata().getOrDefault("fullname", "") + " " +
                    document.getMetadata().getOrDefault("email", "") +  document.getMetadata().getOrDefault("phone", "");
            List<Double> embedding = embeddingClient.embed(textToEmbed);
            embeddings.put((String)document.getMetadata().get("id"), embedding);
            document.setEmbedding(embedding);
        }

        // Step 5: Add documents with embeddings to vector store
        vectorStore.add(documents);
    }

    /**
     * Perform a vector similarity search from pre-saved embeddings.
     *
     * @param query            Query string to search for.
     * @param embeddingsFilePath Path to the file containing saved embeddings.
     * @return List of matched documents.
     * @throws IOException If the file cannot be read.
     */
    public List<Document> searchFromSavedEmbeddings(String query, String metadataFilePath, String embeddingsFilePath) throws IOException {
        // Step 3: Perform similarity search
        return vectorStore.similaritySearch(
                SearchRequest.defaults()
                        .withQuery(query)
                        .withTopK(1) // Retrieve top 3 results
        );
    }

    /**
     * Custom metadata generator for embedding.
     */
    public class PersonMetadataGenerator implements JsonMetadataGenerator {
        @Override
        public Map<String, Object> generate(Map<String, Object> jsonMap) {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("id", jsonMap.getOrDefault("id", ""));
            metadata.put("prefix", jsonMap.getOrDefault("prefix", ""));
            metadata.put("firstname", jsonMap.getOrDefault("firstname", ""));
            metadata.put("lastname", jsonMap.getOrDefault("lastname", ""));
            metadata.put("fullname", jsonMap.getOrDefault("fullname", ""));
            metadata.put("email", jsonMap.getOrDefault("email", ""));
            metadata.put("phone", jsonMap.getOrDefault("phone", ""));
            return metadata;
        }
    }
}

