package lab.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkersConfig {

    /** The pool the listener hands records to. */
    @Bean
    ExecutorService workers(@Value("${lab.threads}") int threads) {
        return Executors.newFixedThreadPool(threads);
    }
}
