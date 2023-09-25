package com.mmc.kafkaplayground;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@SpringBootApplication
@EnableConfigurationProperties
@ConfigurationPropertiesScan
public class KafkaPlaygroundApplication implements CommandLineRunner {

    @Autowired
    List<App> apps;

    public static void main(String[] args) {
        SpringApplication.run(KafkaPlaygroundApplication.class, args);
    }

    @Override
    public void run(String... args) {
        apps.forEach(app -> CompletableFuture.runAsync(app::run)
                .exceptionally(throwable -> {
                    throwable.printStackTrace();
                    System.exit(1);
                    return null;
                }));
    }
}
