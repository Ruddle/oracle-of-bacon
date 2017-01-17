package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

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
                "{ \"csv\" : { \"properties\" : { \"firstname\" : {\"type\" : \"string\", \"store\" : \"yes\"} } } }"
        ).build();
        client.execute(putMapping);

        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader.lines()
                    .forEach(line -> {
                        if(line.substring(0,1).contains("\"")){
                            Index index = new Index.Builder("{\"firstname\":\""+line.substring(1,line.length()-1)+"\"}").index("actors").type("csv").build();
                            try {
                                client.execute(index);
                            //    System.out.println(line);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
        }

        System.out.println("Inserted total of " + count.get() + " actors");
    }
}
