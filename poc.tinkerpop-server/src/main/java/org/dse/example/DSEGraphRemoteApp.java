package org.dse.example;

import java.io.Closeable;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;


import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DSEGraphRemoteApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(DSEGraphRemoteApp.class);

    static class Session implements Closeable {
        final GraphTraversalSource g;
        final DseSession dse;
        final private DseCluster dseCluster;

        protected Session( String host, String graph_name) {

            dseCluster = DseCluster.builder()
                    .addContactPoint(host)
                    .build();
            this.dse = dseCluster.connect();

            g = DseGraph.traversal(dse, new GraphOptions().setGraphName(graph_name));

        }

        @Override
        public void close() throws IOException {
            dse.close();
            dseCluster.close();
        }

        static Session open( String host, String graph_name ) {
            return new Session( host, graph_name);
        }


    }


    public static void createGraph(GraphTraversalSource g){
        g.addV("SimpleCurrency").as("c")
                .property("symbol","GBP")
                .property("rank","1")
                .group()
                .next();
    }

    public static void createGraphFamily(GraphTraversalSource g){
        final Graph gh = g.getGraph();

        final Vertex marko = gh.addVertex("name");

        final Vertex vadas = gh.addVertex("name", "vadas", "age", 27);
        final Vertex lop = gh.addVertex("name", "lop", "lang", "java");
        final Vertex josh = gh.addVertex("name", "josh", "age", 32);
        final Vertex ripple = gh.addVertex("name", "ripple", "lang", "java");
        final Vertex peter = gh.addVertex("name", "peter", "age", 35);
        marko.addEdge("knows", vadas, "weight", 0.5f);
        marko.addEdge("knows", josh, "weight", 1.0f);
        marko.addEdge("created", lop, "weight", 0.4f);
        josh.addEdge("created", ripple, "weight", 1.0f);
        josh.addEdge("created", lop, "weight", 0.4f);
        peter.addEdge("created", lop, "weight", 0.2f);
    }




    public static void main(String[] args) throws Exception {

        final Path confPath = Paths.get("conf", "dse.properties");

        final Configuration conf = new PropertiesConfiguration(confPath.toFile());


        final Session s = Session.open(conf.getString("host", "192.168.1.13"), conf.getString("graph", "DSE_GRAPH_QUICKSTART"));

        //createGraph(s.g);

        //createGraphFamily(s.g);

        LOGGER.info("Number of vertexes" + s.g.V().count().next());

        Traversal<Vertex, String> t1 = s.g.V().values("symbol");

        while (t1.hasNext()) {
            LOGGER.info("value: " + t1.next());
        }

        Traversal<Vertex, String> t2 = s.g.V().hasLabel("SimpleCurrency").values();

        while (t2.hasNext()) {
            /*
            Map<String, List> m= t2.next();
            if (m.size()>0){
                m.get(0)
            }
            */
            LOGGER.info("value: " + t2.next());
        }

        Traversal<Vertex, Map<String, List>> t3 = s.g.V().hasLabel("SimpleCurrency").valueMap();

        while (t3.hasNext()) {
            Map<String, List> m = t3.next();
            if (m.size() > 0) {
                    for(String key: m.keySet())
                         LOGGER.info("key: " + key + " value:"+m.get(key).get(0));
            }
        }
    }
}
