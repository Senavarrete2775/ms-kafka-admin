package cl.tnt.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** Datos para crear un tópico, con validaciones de entrada. */
public class TopicRequestDTO {

    @NotBlank(message = "El nombre del tópico es obligatorio y no puede contener solo espacios.")
    @Size(max = 249, message = "El nombre del tópico no puede superar los 249 caracteres.")
    @Pattern(regexp = "^[a-zA-Z0-9._-]*$",
            message = "El nombre solo puede contener letras, números, punto, guion y guion bajo.")
    private String name;

    @Min(value = 1, message = "El tópico necesita al menos 1 partición.")
    @Max(value = 50, message = "El máximo permitido es de 50 particiones.")
    private int partitions = 3;

    @Min(value = 1, message = "El factor de replicación mínimo es 1.")
    @Max(value = 5, message = "El factor de replicación máximo es 5.")
    private short replicationFactor = 3;

    @Pattern(regexp = "^(delete|compact|compact,delete)$",
            message = "cleanupPolicy debe ser 'delete', 'compact' o 'compact,delete'.")
    private String cleanupPolicy = "delete";

    @Min(value = -1, message = "retentionMs debe ser -1 (infinito) o un valor positivo en milisegundos.")
    private long retentionMs = 604_800_000L;

    @Min(value = 1, message = "minInsyncReplicas debe ser al menos 1.")
    private Integer minInsyncReplicas;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }
    public short getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(short replicationFactor) { this.replicationFactor = replicationFactor; }
    public String getCleanupPolicy() { return cleanupPolicy; }
    public void setCleanupPolicy(String cleanupPolicy) { this.cleanupPolicy = cleanupPolicy; }
    public long getRetentionMs() { return retentionMs; }
    public void setRetentionMs(long retentionMs) { this.retentionMs = retentionMs; }
    public Integer getMinInsyncReplicas() { return minInsyncReplicas; }
    public void setMinInsyncReplicas(Integer minInsyncReplicas) { this.minInsyncReplicas = minInsyncReplicas; }
}
