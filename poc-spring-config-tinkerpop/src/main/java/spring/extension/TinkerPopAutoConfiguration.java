package spring.extension;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 *  A simple auto configuration for tinkerpop repository environment. The configuration is enabled
 *  by spring.factories using 'tinkerpop' as active profile.
 *
 *  @author Antonio Valentino
 */

@Configuration
@Profile("tinkerpop")
public class TinkerPopAutoConfiguration {


    @Bean
    @ConditionalOnMissingBean(TinkerPopEnvironmentRepository.class)
    public TinkerPopEnvironmentRepository tinkerPopEnvironmentRepository()
    {
        return new TinkerPopEnvironmentRepository();
    }
}
