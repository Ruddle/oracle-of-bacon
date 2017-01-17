package com.serli.oracle.of.bacon.repository;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.List;

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
                "      \"query\": {\n" +
                "        \"match\": {\n" +
                "           \"firstname\": {\n" +
                "             \"query\" : \"" + searchQuery + "\",\n" +
                "             \"fuzziness\" : \"auto\" \n" +
                "           }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Search search = new Search.Builder(json).addIndex("actors").build();
        SearchResult sr = jestClient.execute(search);
        List<String> rs = sr.getSourceAsStringList();
        for (int i = 0; i < rs.size(); i++) {
            rs.set(i, rs.get(i).substring(14, rs.get(i).length() - 2));
        }
        return rs;
    }

}
