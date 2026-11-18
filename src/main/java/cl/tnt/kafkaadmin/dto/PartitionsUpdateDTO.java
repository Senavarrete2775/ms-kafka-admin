package cl.tnt.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/** Nuevo total de particiones (Kafka solo permite aumentarlas). */
public class PartitionsUpdateDTO {

    @Min(value = 1, message = "El total de particiones debe ser al menos 1.")
    @Max(value = 50, message = "El máximo permitido es de 50 particiones.")
    private int partitions;

    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }
}
