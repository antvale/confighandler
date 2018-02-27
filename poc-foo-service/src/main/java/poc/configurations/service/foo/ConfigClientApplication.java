package poc.configurations.service.foo;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@SpringBootApplication
public class ConfigClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConfigClientApplication.class, args);
    }
}

@RefreshScope
@RestController
class MessageRestController {

    @Value("${welcome:Hello default}")
    private String message;

    @Value("${isBar:true}")
    private boolean isBar;

    @RequestMapping("/message")
    String getMessage() {
        String result;

        if (isBar)
            result=this.message +" I'm Bar";
        else
            result=this.message +" I'm Foo";
        return result;
    }
}
