package org.apache.metamodel.membrane.app.config;

import org.apache.metamodel.membrane.swagger.invoker.JSON;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class JacksonConfig {

    @Bean(name = "objectMapper")
    public ObjectMapper objectMapper() {
        // use the JSON class from swagger-codegen
        final JSON json = new JSON();
        return json.getContext(Object.class);
    }
}
