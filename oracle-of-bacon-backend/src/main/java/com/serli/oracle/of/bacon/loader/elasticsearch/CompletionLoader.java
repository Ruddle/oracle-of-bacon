package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Suggest;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];
        JestClient client = ElasticSearchRepository.createClient();
        client.execute(new CreateIndex.Builder("actors").build());
        PutMapping putMapping = new PutMapping.Builder(
                "actors",
                "csv",
                "{ \"csv\" : { \"properties\" : { \"firstname\" : {\"type\" : \"completion\", \"store\" : \"yes\"} } } }"
        ).build();
        client.execute(putMapping);

        final int bulksize = 10000;
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            List<String> list = bufferedReader.lines().map(String::new).collect(Collectors.toCollection(ArrayList::new));
            Bulk.Builder bulkBuilder = new Bulk.Builder();
            System.out.println("Sending by bulk");
            for (int i = 0; i < list.size(); i++) {
                bulkBuilder = bulkBuilder.addAction(
                        new Index.Builder(
                                "{\"firstname\":\"" + list.get(i).substring(1, list.get(i).length() - 1) + "\"}"
                        ).build()
                );
                if (i % bulksize == bulksize - 1) {
                    client.execute(bulkBuilder.build());
                    bulkBuilder = new Bulk.Builder()
                            .defaultIndex("actors")
                            .defaultType("csv");
                }
            }
            client.execute(bulkBuilder.build());
            System.out.println("Inserted total of " + list.size() + " actors");
        }
    }
}
