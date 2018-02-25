package org.dse.example;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;

public class DSEGraphRemoteApp {
    //private static final Logger LOGGER = LoggerFactory.getLogger(DSEGraphRemoteApp.class);

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

    public static void main(String[] args) throws Exception {

        final Path confPath = Paths.get( "conf", "dse.properties");

        final Configuration conf = new PropertiesConfiguration(confPath.toFile());

        try( final Session s =Session.open(conf.getString("host", "localhost"), conf.getString("graph","DSE_GRAPH_QUICKSTART"))) {

                //s.g.V().count()

        }
    }

}
