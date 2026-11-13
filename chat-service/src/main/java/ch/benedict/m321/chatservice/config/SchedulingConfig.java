package ch.benedict.m321.chatservice.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Schaltet @Scheduled ein. Ohne diese Klasse würde der HeartbeatSender nie
 * laufen, Spring ignoriert @Scheduled sonst stillschweigend.
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
