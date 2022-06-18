package dev.marcinromanowski.testutils

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import com.jayway.jsonpath.ParseContext
import com.jayway.jsonpath.spi.json.JacksonJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import groovy.transform.CompileStatic

@CompileStatic
class JsonPathConfiguration {

    private static ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES))
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.NONE)
                .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.ANY)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
                .configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, true)
                .configure(MapperFeature.AUTO_DETECT_FIELDS, true)
    }

    static ParseContext parser() {
        return JsonPath.using(configure())
    }

    static Configuration configure() {
        def mapper = objectMapper()
        return Configuration.builder()
                .jsonProvider(new JacksonJsonProvider(mapper))
                .mappingProvider(new JacksonMappingProvider(mapper))
                .options(EnumSet.noneOf(Option.class))
                .build()
    }

}
