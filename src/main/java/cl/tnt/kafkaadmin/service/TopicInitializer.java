package cl.tnt.kafkaadmin.service;

import cl.tnt.kafkaadmin.config.KafkaTopicsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** Al iniciar crea los tópicos definidos en application.yml si aún no existen. */
@Component
public class TopicInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TopicInitializer.class);

    private final KafkaAdminService service;
    private final KafkaTopicsProperties props;

    public TopicInitializer(KafkaAdminService service, KafkaTopicsProperties props) {
        this.service = service;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (KafkaTopicsProperties.TopicDefinition def : props.topics()) {
            try {
                service.crearTopicoSiNoExiste(def);
            } catch (Exception e) {
                log.error("No se pudo asegurar el tópico '{}': {}. Puede crearlo con POST /topics.",
                        def.name(), e.getMessage());
            }
        }
    }
}
