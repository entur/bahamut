package org.entur.bahamut;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;

@SpringBootApplication
@EnableTask
public class BahamutApplication {

    public static void main(String[] args) {
        SpringApplication.run(BahamutApplication.class, args);
    }
}