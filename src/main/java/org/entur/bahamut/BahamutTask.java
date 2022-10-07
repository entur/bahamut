package org.entur.bahamut;

import org.apache.camel.CamelContext;
import org.apache.camel.FluentProducerTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class BahamutTask implements CommandLineRunner {

    private final CamelContext camelContext;

    public BahamutTask(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    @Override
    public void run(String... args) throws Exception {
        FluentProducerTemplate fluentProducerTemplate = camelContext.createFluentProducerTemplate();
        fluentProducerTemplate.to("direct:makeCSV").request();
    }
}
