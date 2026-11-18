package cl.tnt.kafkaadmin.controller;

import cl.tnt.kafkaadmin.dto.PartitionsUpdateDTO;
import cl.tnt.kafkaadmin.dto.TopicConfigUpdateDTO;
import cl.tnt.kafkaadmin.dto.TopicRequestDTO;
import cl.tnt.kafkaadmin.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** API REST de administración de Kafka. Toda la lógica está en KafkaAdminService. */
@RestController
@CrossOrigin(origins = "*", methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.PUT,
        RequestMethod.DELETE, RequestMethod.OPTIONS})
@RequestMapping
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "servicio", "ms-kafka-admin",
                "estado", "ONLINE",
                "descripcion", "Microservicio Administrador de Kafka (tópicos, configuraciones y consumer groups)",
                "timestamp", System.currentTimeMillis()));
    }

    @GetMapping("/cluster")
    public ResponseEntity<Map<String, Object>> cluster() {
        return ResponseEntity.ok(service.describirCluster());
    }

    // ---------- Tópicos ----------

    @GetMapping("/topics")
    public ResponseEntity<List<Map<String, Object>>> listarTopicos() {
        return ResponseEntity.ok(service.listarTopicos());
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> crearTopico(@Valid @RequestBody TopicRequestDTO request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(service.crearTopico(request));
    }

    @GetMapping("/topics/{name}")
    public ResponseEntity<Map<String, Object>> obtenerTopico(@PathVariable("name") String name) {
        return ResponseEntity.ok(service.obtenerTopico(name));
    }

    @PutMapping("/topics/{name}/config")
    public ResponseEntity<Map<String, Object>> actualizarConfig(@PathVariable("name") String name,
                                                                @Valid @RequestBody TopicConfigUpdateDTO request) {
        return ResponseEntity.ok(service.actualizarConfiguracion(name, request.getConfigs()));
    }

    @PutMapping("/topics/{name}/partitions")
    public ResponseEntity<Map<String, Object>> aumentarParticiones(@PathVariable("name") String name,
                                                                   @Valid @RequestBody PartitionsUpdateDTO request) {
        return ResponseEntity.ok(service.aumentarParticiones(name, request.getPartitions()));
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Map<String, Object>> eliminarTopico(@PathVariable("name") String name) {
        service.eliminarTopico(name);
        return ResponseEntity.ok(Map.of("status", "OK",
                "mensaje", "El tópico '" + name.trim() + "' fue eliminado."));
    }

    // ---------- Consumer groups ----------

    @GetMapping("/consumer-groups")
    public ResponseEntity<List<Map<String, Object>>> listarGrupos() {
        return ResponseEntity.ok(service.listarGrupos());
    }

    @GetMapping("/consumer-groups/{groupId}")
    public ResponseEntity<Map<String, Object>> obtenerGrupo(@PathVariable("groupId") String groupId) {
        return ResponseEntity.ok(service.obtenerGrupo(groupId));
    }
}
