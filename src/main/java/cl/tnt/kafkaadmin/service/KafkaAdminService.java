package cl.tnt.kafkaadmin.service;

import cl.tnt.kafkaadmin.config.KafkaTopicsProperties.TopicDefinition;
import cl.tnt.kafkaadmin.dto.TopicRequestDTO;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Encapsula toda la administración de Kafka: tópicos, configuraciones, particiones,
 * consumer groups e información del clúster. Los controladores solo llaman a estos métodos.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);
    private static final long TIMEOUT_SEGUNDOS = 15;

    /** Configuraciones de tópico que se pueden consultar y modificar desde la API. */
    private static final Set<String> CONFIGS_PERMITIDAS = Set.of(
            "cleanup.policy", "retention.ms", "retention.bytes",
            "min.insync.replicas", "segment.bytes", "max.message.bytes");

    private final Admin admin;

    public KafkaAdminService(Admin admin) {
        this.admin = admin;
    }

    // ===================== CLÚSTER =====================

    public Map<String, Object> describirCluster() {
        var cluster = admin.describeCluster();
        Collection<Node> nodos = esperar(cluster.nodes(), "describir el clúster");
        Node controlador = esperar(cluster.controller(), "obtener el controlador");
        List<Map<String, Object>> brokers = new ArrayList<>();
        for (Node n : nodos) {
            Map<String, Object> b = new LinkedHashMap<>();
            b.put("id", n.id());
            b.put("host", n.host());
            b.put("puerto", n.port());
            brokers.add(b);
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("clusterId", esperar(cluster.clusterId(), "obtener el id del clúster"));
        resp.put("totalBrokers", brokers.size());
        resp.put("controladorId", controlador != null ? controlador.id() : null);
        resp.put("brokers", brokers);
        return resp;
    }

    // ===================== TÓPICOS =====================

    public Map<String, Object> crearTopico(TopicRequestDTO dto) {
        String nombre = dto.getName().trim();
        if (dto.getMinInsyncReplicas() != null && dto.getMinInsyncReplicas() > dto.getReplicationFactor()) {
            throw new IllegalArgumentException(
                    "minInsyncReplicas no puede ser mayor que el factor de replicación.");
        }
        Map<String, String> configs = new LinkedHashMap<>();
        configs.put("cleanup.policy", dto.getCleanupPolicy());
        configs.put("retention.ms", String.valueOf(dto.getRetentionMs()));
        if (dto.getMinInsyncReplicas() != null) {
            configs.put("min.insync.replicas", String.valueOf(dto.getMinInsyncReplicas()));
        }
        NewTopic nuevo = new NewTopic(nombre, dto.getPartitions(), dto.getReplicationFactor()).configs(configs);
        log.info("Creando tópico '{}' (particiones={}, réplicas={}, configs={})",
                nombre, dto.getPartitions(), dto.getReplicationFactor(), configs);
        esperar(admin.createTopics(List.of(nuevo)).all(), "crear el tópico '" + nombre + "'");
        return describirTopicos(List.of(nombre)).get(0);
    }

    /** Usado al iniciar: crea el tópico solo si todavía no existe. */
    public boolean crearTopicoSiNoExiste(TopicDefinition def) {
        Set<String> existentes = esperar(admin.listTopics().names(), "listar tópicos");
        if (existentes.contains(def.name())) {
            log.info("Tópico '{}' ya existe, no se modifica.", def.name());
            return false;
        }
        NewTopic nuevo = new NewTopic(def.name(), def.partitions(), def.replicationFactor()).configs(Map.of(
                "cleanup.policy", def.cleanupPolicy(),
                "retention.ms", String.valueOf(def.retentionMs()),
                "min.insync.replicas", String.valueOf(def.minInsyncReplicas())));
        esperar(admin.createTopics(List.of(nuevo)).all(), "crear el tópico '" + def.name() + "'");
        log.info("Tópico '{}' creado: {} particiones, {} réplicas, cleanup.policy={}.",
                def.name(), def.partitions(), def.replicationFactor(), def.cleanupPolicy());
        return true;
    }

    public List<Map<String, Object>> listarTopicos() {
        Set<String> nombres = esperar(
                admin.listTopics(new ListTopicsOptions().listInternal(false)).names(), "listar tópicos");
        if (nombres.isEmpty()) {
            return List.of();
        }
        return describirTopicos(nombres.stream().sorted().toList());
    }

    public Map<String, Object> obtenerTopico(String nombre) {
        return describirTopicos(List.of(nombre.trim())).get(0);
    }

    public Map<String, Object> actualizarConfiguracion(String nombre, Map<String, String> configs) {
        String topico = nombre.trim();
        for (String clave : configs.keySet()) {
            if (!CONFIGS_PERMITIDAS.contains(clave)) {
                throw new IllegalArgumentException("Configuración no permitida: '" + clave
                        + "'. Permitidas: " + new TreeSet<>(CONFIGS_PERMITIDAS));
            }
        }
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, topico);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        configs.forEach((k, v) -> ops.add(new AlterConfigOp(new ConfigEntry(k, v), AlterConfigOp.OpType.SET)));
        log.info("Modificando configuración del tópico '{}': {}", topico, configs);
        esperar(admin.incrementalAlterConfigs(Map.of(recurso, ops)).all(),
                "modificar la configuración de '" + topico + "'");
        return obtenerTopico(topico);
    }

    public Map<String, Object> aumentarParticiones(String nombre, int total) {
        String topico = nombre.trim();
        log.info("Aumentando particiones del tópico '{}' a {}", topico, total);
        esperar(admin.createPartitions(Map.of(topico, NewPartitions.increaseTo(total))).all(),
                "aumentar las particiones de '" + topico + "'");
        return obtenerTopico(topico);
    }

    public void eliminarTopico(String nombre) {
        String topico = nombre.trim();
        log.info("Eliminando tópico '{}'", topico);
        esperar(admin.deleteTopics(List.of(topico)).all(), "eliminar el tópico '" + topico + "'");
    }

    // ===================== CONSUMER GROUPS =====================

    public List<Map<String, Object>> listarGrupos() {
        var grupos = esperar(admin.listConsumerGroups().all(), "listar consumer groups");
        List<String> ids = grupos.stream().map(g -> g.groupId()).sorted().toList();
        return ids.isEmpty() ? List.of() : describirGrupos(ids);
    }

    public Map<String, Object> obtenerGrupo(String groupId) {
        List<Map<String, Object>> r = describirGrupos(List.of(groupId.trim()));
        if (r.isEmpty() || "DEAD".equals(String.valueOf(r.get(0).get("estado")))) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "El consumer group '" + groupId + "' no existe.");
        }
        return r.get(0);
    }

    // ===================== INTERNOS =====================

    private List<Map<String, Object>> describirGrupos(List<String> ids) {
        Map<String, ConsumerGroupDescription> descs =
                esperar(admin.describeConsumerGroups(ids).all(), "describir consumer groups");
        List<Map<String, Object>> resultado = new ArrayList<>();
        for (String id : ids) {
            ConsumerGroupDescription d = descs.get(id);
            if (d == null) {
                continue;
            }
            List<Map<String, Object>> miembros = new ArrayList<>();
            for (MemberDescription m : d.members()) {
                Map<String, Object> mm = new LinkedHashMap<>();
                mm.put("clientId", m.clientId());
                mm.put("host", m.host());
                mm.put("particiones", m.assignment().topicPartitions().stream()
                        .map(tp -> tp.topic() + "-" + tp.partition()).sorted().toList());
                miembros.add(mm);
            }
            Map<String, Object> g = new LinkedHashMap<>();
            g.put("grupo", d.groupId());
            g.put("estado", String.valueOf(d.state()));
            g.put("totalMiembros", miembros.size());
            g.put("miembros", miembros);
            resultado.add(g);
        }
        return resultado;
    }

    private List<Map<String, Object>> describirTopicos(List<String> nombres) {
        Map<String, TopicDescription> descs =
                esperar(admin.describeTopics(nombres).allTopicNames(), "describir tópicos");
        List<ConfigResource> recursos = nombres.stream()
                .map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n)).toList();
        Map<ConfigResource, Config> configs = esperar(admin.describeConfigs(recursos).all(), "leer configuraciones");

        List<Map<String, Object>> resultado = new ArrayList<>();
        for (String nombre : nombres) {
            TopicDescription td = descs.get(nombre);
            List<Map<String, Object>> particiones = new ArrayList<>();
            for (TopicPartitionInfo p : td.partitions()) {
                Map<String, Object> pm = new LinkedHashMap<>();
                pm.put("particion", p.partition());
                pm.put("lider", p.leader() != null ? p.leader().id() : null);
                pm.put("replicas", p.replicas().stream().map(Node::id).toList());
                pm.put("isr", p.isr().stream().map(Node::id).toList());
                particiones.add(pm);
            }
            Config cfg = configs.get(new ConfigResource(ConfigResource.Type.TOPIC, nombre));
            Map<String, String> cfgMostrada = new LinkedHashMap<>();
            for (String clave : List.of("cleanup.policy", "retention.ms", "min.insync.replicas")) {
                ConfigEntry e = cfg != null ? cfg.get(clave) : null;
                cfgMostrada.put(clave, e != null ? e.value() : null);
            }
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("nombre", nombre);
            t.put("totalParticiones", td.partitions().size());
            t.put("factorReplicacion", td.partitions().isEmpty() ? 0 : td.partitions().get(0).replicas().size());
            t.put("configuracion", cfgMostrada);
            t.put("particiones", particiones);
            resultado.add(t);
        }
        return resultado;
    }

    private <T> T esperar(KafkaFuture<T> future, String operacion) {
        try {
            return future.get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Operación interrumpida al " + operacion);
        } catch (TimeoutException e) {
            throw new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT,
                    "Kafka no respondió a tiempo al " + operacion + ". Verifique que los brokers estén activos.");
        } catch (ExecutionException e) {
            throw traducir(e.getCause(), operacion);
        }
    }

    private ResponseStatusException traducir(Throwable causa, String operacion) {
        String detalle = causa != null ? causa.getMessage() : "error desconocido";
        if (causa instanceof TopicExistsException) {
            return new ResponseStatusException(HttpStatus.CONFLICT, "El tópico ya existe: " + detalle);
        }
        if (causa instanceof UnknownTopicOrPartitionException) {
            return new ResponseStatusException(HttpStatus.NOT_FOUND, "El tópico no existe: " + detalle);
        }
        if (causa instanceof InvalidReplicationFactorException || causa instanceof InvalidConfigurationException
                || causa instanceof InvalidPartitionsException || causa instanceof InvalidRequestException
                || causa instanceof InvalidTopicException) {
            return new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Kafka rechazó la solicitud al " + operacion + ": " + detalle);
        }
        log.error("Error de Kafka al {}: {}", operacion, detalle, causa);
        return new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Error de Kafka al " + operacion + ": " + detalle);
    }
}
