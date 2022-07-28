package org.entur.bahamut.camel.csv;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;

public record CSVValue(Object value, boolean json) {
    @Override
    public String toString() {
        if (value == null) {
            return "";
        } else if (json) {
            try {
                var mapper = new ObjectMapper();
                mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                var writer = new StringWriter();
                mapper.writeValue(writer, value);
                return writer.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return value.toString();
        }
    }
}
