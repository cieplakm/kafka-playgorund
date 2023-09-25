package com.mmc.kafkaplayground.components.emailsender;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "email.sender")
@Data
public class EmailSenderProperties {

    String username;
    String password;
    String host;
    String port;
}
