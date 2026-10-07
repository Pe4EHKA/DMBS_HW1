package ru.gamehub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class GameHubApplication {
    public static void main(String[] args) {
        SpringApplication.run(GameHubApplication.class, args);
    }
}
