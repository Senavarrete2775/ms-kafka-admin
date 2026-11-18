package cl.tnt.kafkaadmin.dto;

import jakarta.validation.constraints.NotEmpty;

import java.util.Map;

/** Configuraciones a modificar en un tópico (ej: {"retention.ms": "86400000"}). */
public class TopicConfigUpdateDTO {

    @NotEmpty(message = "Debe indicar al menos una configuración a modificar.")
    private Map<String, String> configs;

    public Map<String, String> getConfigs() { return configs; }
    public void setConfigs(Map<String, String> configs) { this.configs = configs; }
}
