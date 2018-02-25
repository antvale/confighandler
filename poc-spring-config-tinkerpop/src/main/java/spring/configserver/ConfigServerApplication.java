package spring.configserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * This is a simple class used to start spring config-server through spring boot.
 *
 * @author Antonio Valentino
 */
@EnableConfigServer
@SpringBootApplication
public class ConfigServerApplication {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigServerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ConfigServerApplication.class, args);
    }
}