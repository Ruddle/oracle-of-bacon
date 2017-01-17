package com.serli.oracle.of.bacon.repository;


import com.sun.javafx.geom.Edge;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import java.util.ArrayList;
import java.util.List;


public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "a"));
    }

    public String getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();

        String query = "MATCH p=shortestPath(\n" +
                "  (bacon:Actor {name:\"Hanks, Tom\"})-[*]-(act:Actor {name:\"" + actorName + "\"})\n" +
                ")\n" +
                "RETURN p";

        ArrayList<GraphNode> listNode = new ArrayList<GraphNode>();
        ArrayList<GraphEdge> listEdge = new ArrayList<GraphEdge>();

        StatementResult result = session.run(query);
        while (result.hasNext()) {
            Record record = result.next();

            Path p = record.get("p").asPath();

            for (Node n : p.nodes()) {
                String label = "Movie";
                for(String s : n.labels()) {
                    if (s.equals("Actor"))
                        label = s;
                }
                listNode.add(new GraphNode(n.id(), (label.equals("Actor")? n.get("name") :n.get("title")).asString() , label ));
            }

            for (Relationship r : p.relationships()) {
                listEdge.add(new GraphEdge(r.id(), r.startNodeId(), r.endNodeId(), "played_in"));
            }


        }
        session.close();



        String res = "[";

        for (GraphNode n : listNode) {
            res+=  n + ",";
        }

        for (GraphEdge n : listEdge) {
            res+= n + ",";
        }
        res =res.substring(0, res.length()-1);
        res+= "]";
        System.out.println(res);
        return res;
    }

    private static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }

        public String toString() {
            return "{\n" +
                    "\"data\": {\n" +
                    "\"id\": " + id + ",\n" +
                    "\"type\": \""+type+"\",\n" +
                    "\"value\": \"" + value + "\"\n" +
                    "}\n" +
                    "}";
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }

        public String toString() {
            return "{\n" +
                    "\"data\": {\n" +
                    "\"id\": "+id+",\n" +
                    "\"source\": "+source+",\n" +
                    "\"target\": "+target+",\n" +
                    "\"value\": \"PLAYED_IN\"\n" +
                    "}\n" +
                    "}";
        }
    }
}
