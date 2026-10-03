package lab.common;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Minimal Spring Boot application. Which listener is active is chosen by the profile: broken or fixed. */
@SpringBootApplication(scanBasePackages = "lab")
public class LabApp {
}
