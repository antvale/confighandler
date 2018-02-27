package spring.extension;

import org.springframework.cloud.config.server.environment.EnvironmentRepository;
import org.springframework.core.Ordered;
import org.springframework.cloud.config.environment.Environment;
import org.springframework.util.StringUtils;
import org.springframework.cloud.config.environment.PropertySource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class implements the repository environment handling the configuration through tinkerpop framework
 *
 * @author Antonio Valentino
 */
public class TinkerPopEnvironmentRepository implements EnvironmentRepository, Ordered {

    private static final Logger LOGGER = LoggerFactory.getLogger(TinkerPopEnvironmentRepository.class);

    private int order = Ordered.LOWEST_PRECEDENCE + 10;

    public TinkerPopEnvironmentRepository(){}

    @Override
    public Environment findOne(String application, String profile, String label) {

        String config = application;
        if (StringUtils.isEmpty(label)) {
            label = "master";
        }
        if (StringUtils.isEmpty(profile)) {
            profile = "default";
        }
        if (!profile.startsWith("default")) {
            profile = "default," + profile;
        }
        String[] profiles = StringUtils.commaDelimitedListToStringArray(profile);
        Environment environment = new Environment(application, profiles, label, null,
                null);
        if (!config.startsWith("application")) {
            config = "application," + config;
        }

        List<String> applications = new ArrayList<String>(new LinkedHashSet<>(
                Arrays.asList(StringUtils.commaDelimitedListToStringArray(config))));
        List<String> envs = new ArrayList<String>(new LinkedHashSet<>(Arrays.asList(profiles)));
        Collections.reverse(applications);
        Collections.reverse(envs);

        for (String app : applications) {
            for (String env : envs) {
                try {
                    Map<String, String> map = SimpleDSEGraphClient.getInstance().simpleSearch(app);
                    environment.add(new PropertySource(app + "-" + env, map));
                } catch (Exception e){
                    LOGGER.error("Error while processing the dse graph for app "+app,e);
                }


            }
        }

        return environment;

    }


    @Override
    public int getOrder() {
        return order;
    }


}
