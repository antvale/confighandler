package spring.extension;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SimpleDSEGraphClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleDSEGraphClient.class);

    private static volatile SimpleDSEGraphClient instance;

    private static GraphTraversalSource g;

    enum T {
        name,
        type,
        label,
        value_long,
        value_double,
        value_string,
        value_bool,
        value_timestamp,
        updated

        ;


        private static final Bindings bindings = Bindings.instance();

        String of( String value ) {
            return bindings.of(name(), value);
        }
    }


    static final String v_application	= T.label.of("application");
    static final String v_attribute	= T.label.of("attribute");
    static final String e_attribute = T.label.of("has_attribute");


    public SimpleDSEGraphClient() throws Exception{
        g=this. getConnection().g;
    }

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


        static Session open(String host, String graph_name ) {
            return new Session( host, graph_name);
        }


    }


    public void initialize(){
        drop();
        createSimpleGraph("greetings");
    }

    public void drop(){

        LOGGER.info("Attempt to drop the graph of "+ g.V().count().next()+" items");

        try {
            g.V().drop().iterate();
        }
        catch( Exception ex ) {
            LOGGER.error( "Error while deleting the graph items" ,ex);
        } finally{
            LOGGER.info("Remaining "+ g.V().count().next() +" items");
        }

    }


    public void createSimpleGraph(String app){

        g.addV(v_application).as("f").property("name",app)
                .addV(v_attribute)
                .property("welcome", "Hello World!")
                .addE(e_attribute).from("f").inV()
                .addV(v_attribute)
                .property("isBar","false")
                .addE(e_attribute).from("f").inV().next();
    }


    public Map<String,String> simpleSearch(String app){

        Map<String, String> result=new HashMap<String,String>();

        final Optional<Vertex> v=g.V().hasLabel(v_application).has("name",app).tryNext();

        if(v.isPresent() ) {
            LOGGER.info("there is at least a property");
            Traversal<Vertex,Map<String,List>> a=g.V(v.get().id()).out(e_attribute).valueMap();
            while (a.hasNext()) {
                Map<String, List> m = a.next();
                if (m.size() > 0) {
                    for(String key: m.keySet()) {
                        String value= (String) m.get(key).get(0);
                        //LOGGER.info("key: " + key + " value:" + m.get(key).get(0));
                        result.put(key,value);
                    }
                }

            }
        }
        return result;
    }


    private Session getConnection() throws Exception {
        final Path confPath = Paths.get("conf", "dse.properties");
        final Configuration conf = new PropertiesConfiguration(confPath.toFile());

        final Session s = Session.open(conf.getString("host", "localhost"), conf.getString("graph", "DSE_GRAPH_QUICKSTART"));

        return s;
    }


    public static SimpleDSEGraphClient getInstance() throws Exception{

            if (instance == null) {
                synchronized (SimpleDSEGraphClient.class) {
                    if (instance == null) {
                        instance = new SimpleDSEGraphClient();
                    }
                }
            }
            return instance;
        }

    public Long count(){
        return g.V().count().next();
    }


    public static void main(String[] args) throws Exception {

        LOGGER.info("Number of vertexes" + SimpleDSEGraphClient.getInstance().count());

        SimpleDSEGraphClient.getInstance().initialize();

        LOGGER.info("Number of vertexes" + SimpleDSEGraphClient.getInstance().count());

        Map<String,String> m=SimpleDSEGraphClient.getInstance().simpleSearch("greetings");

        for(String key: m.keySet()) {
            LOGGER.info("key: " + key + " value:" + m.get(key));
        }

    }
}
