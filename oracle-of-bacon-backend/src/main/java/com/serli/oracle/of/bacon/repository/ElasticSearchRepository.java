package com.serli.oracle.of.bacon.repository;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticSearchRepository {

    private final JestClient jestClient;

    public ElasticSearchRepository() {
        jestClient = createClient();

    }

    public static JestClient createClient() {
        JestClient jestClient;
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(60000)
                .build());
        jestClient = factory.getObject();
        return jestClient;
    }

    public List<String> getActorsSuggests(String searchQuery) throws IOException {
        String json = "{\n" +
                "  \"" + "actors" + "\" : {\n" +
                "    \"text\" : \"" + searchQuery + "\",\n" +
                "    \"completion\" : {\n" +
                "      \"field\" : \"suggestname\",\n" +
                "      \"fuzzy\" : {\"fuzziness\":1}\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Suggest suggest = new Suggest.Builder(json).build();
        SuggestResult searchResult = jestClient.execute(suggest);
        List<SuggestResult.Suggestion> suggestions = searchResult.getSuggestions("actors");
        List<String> resultList = new ArrayList<String>();
        for (SuggestResult.Suggestion suggestion : suggestions) {
            List<Map<String, Object>> lmso = suggestion.options;
            for (Map m : lmso) {
                resultList.add(m.get("text").toString());
            }
        }
        return resultList;
    }

}
