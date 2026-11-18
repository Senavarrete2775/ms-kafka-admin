package cl.tnt.kafkaadmin;

import cl.tnt.kafkaadmin.controller.KafkaAdminController;
import cl.tnt.kafkaadmin.service.KafkaAdminService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(KafkaAdminController.class)
class KafkaAdminControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaAdminService service;

    @Test
    void statusDeberiaResponder200() throws Exception {
        mockMvc.perform(get("/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.servicio").value("ms-kafka-admin"));
    }

    @Test
    void listarTopicosDeberiaResponder200() throws Exception {
        Mockito.when(service.listarTopicos()).thenReturn(List.of(Map.of("nombre", "pedidos.eventos")));
        mockMvc.perform(get("/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].nombre").value("pedidos.eventos"));
    }

    @Test
    void crearTopicoValidoDeberiaResponder201() throws Exception {
        Mockito.when(service.crearTopico(any())).thenReturn(Map.of("nombre", "demo.topic"));
        mockMvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo.topic\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.nombre").value("demo.topic"));
    }

    @Test
    void crearTopicoConNombreVacioDeberiaResponder400() throws Exception {
        mockMvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"   \",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detalles.name").exists());
    }

    @Test
    void crearTopicoConParticionesInvalidasDeberiaResponder400() throws Exception {
        mockMvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo\",\"partitions\":0,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detalles.partitions").exists());
    }
}
