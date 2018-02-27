package org.dse.example;

import java.io.Closeable;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;


import jdk.nashorn.internal.runtime.options.Option;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
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
import java.util.Optional;

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



    private static void createParameter(GraphTraversalSource g, final Vertex tpl, String name, Object value  ) {

        g.V(tpl.id()).as("x")
                .property(name, value).next();

    }

    public static Vertex createFeature(GraphTraversalSource g, String feature){

        final Vertex vF=g.addV("feature").as("f").property("name",feature).next();

        return vF;
    }



    public static void addAttribute(GraphTraversalSource g, final Vertex vF, String key, String value){
        g.V(vF.id()).addV("attribute")
                .property(key, value)
                .addE("has_attribute").from("f").inV().next();
    }


    public static void _createSimpleGraph(GraphTraversalSource g){

        g.addV("feature").as("f").property("name","service-foo")
        .addV("attribute")
                .property("welcome", "Hello World!")
                .property("isBar","false")
                .addE("has_attribute").from("f").inV().next();
    }


    public static void createSimpleGraph(GraphTraversalSource g, String feature){

        g.addV("feature").as("f").property("name",feature)
                .addV("attribute")
                .property("welcome", "Hello World!")
                .addE("has_attribute").from("f").inV()
                .addV("attribute")
                .property("isBar","false")
                .addE("has_attribute").from("f").inV().next();
    }


    public static void createGraph4ComplexSpringConfig(GraphTraversalSource g){

        g.addV("Application").as("A").property("name","service-foo")
                .addE("label").property("branch","v1")
                .addV("property").as("P").property("welcome-message","Hello world!")
                                 .property("friend","bar")
                .addE("instance").property("profile","dev").property("friend","foobar")
                .group()
                .next();
    }

    /*
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
    */




    public static void main(String[] args) throws Exception {

        final Path confPath = Paths.get("conf", "dse.properties");

        final Configuration conf = new PropertiesConfiguration(confPath.toFile());


        final Session s = Session.open(conf.getString("host", "192.168.1.13"), conf.getString("graph", "DSE_GRAPH_QUICKSTART"));

        //createGraph(s.g);

        //createGraphFamily(s.g);

        //createGraph4SimpleSpringConfig(s.g);

        //createSimpleGraph(s.g,"greetings");


        LOGGER.info("Number of vertexes" + s.g.V().count().next());

        final Optional<Vertex> v=s.g.V().hasLabel("feature").has("name","greetings").tryNext();

        if(v.isPresent() ) {
            LOGGER.info("there is at least a property");
            Traversal<Vertex,Map<String,List>> a=s.g.V(v.get().id()).out("has_attribute").valueMap();
            while (a.hasNext()) {
                Map<String, List> m = a.next();
                if (m.size() > 0) {
                    for(String key: m.keySet())
                        LOGGER.info("key: " + key + " value:"+m.get(key).get(0));
                }

            }
        }



/*

        Traversal<Vertex, String> t1 = s.g.V().values("symbol");

        while (t1.hasNext()) {
            LOGGER.info("value: " + t1.next());
        }

        Traversal<Vertex, String> t2 = s.g.V().hasLabel("SimpleCurrency").values();

        while (t2.hasNext()) {
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
*/


    }
}
