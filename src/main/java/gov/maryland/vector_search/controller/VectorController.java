package gov.maryland.vector_search.controller;

import gov.maryland.vector_search.services.VectorService;
import gov.maryland.vector_search.services.VectorServiceOld;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("api/v1")
public class VectorController {

    @Autowired
    private VectorService vectorService;

    @PostMapping("/create-embeddings")
    public String createEmbeddings() throws IOException {
        vectorService.createAndSaveEmbeddings("classpath:/person.csv", "classpath:/data/metadata.json", "classpath:/data/embeddings.json");
        return "Embeddings created and saved successfully.";
    }

    @GetMapping("/query")
    public String getQueryResults(@RequestParam String query) throws IOException {
        return vectorService.searchFromSavedEmbeddings(query, "classpath:/data/metadata.json", "classpath:/data/embeddings.json").toString();
    }
}
