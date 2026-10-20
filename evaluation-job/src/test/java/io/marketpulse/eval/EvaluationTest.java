package io.marketpulse.eval;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

class EvaluationTest {
    @Test void readyMarkerAppearsOnlyWhenStreamsOwnsRunningTasks() {
        var bytes=new java.io.ByteArrayOutputStream();var output=new java.io.PrintStream(bytes);
        var listener=StreamsJob.readinessListener("run-test",output);
        listener.onChange(KafkaStreams.State.REBALANCING,KafkaStreams.State.CREATED);
        assertEquals("",bytes.toString());
        listener.onChange(KafkaStreams.State.RUNNING,KafkaStreams.State.REBALANCING);
        assertEquals("EVAL_READY run-test"+System.lineSeparator(),bytes.toString());
    }
    @Test void flinkTransactionClientProvidesConnectorInternalAwait() throws Exception {
        if (System.getProperty("evaluation.engine", "flink").equals("flink")) {
            assertNotNull(Class.forName("org.apache.kafka.clients.producer.internals.TransactionalRequestResult").getDeclaredMethod("await"));
        }
    }
    ObjectNode row(int seq, String id, long time, String kind, int amount) {
        ObjectNode r = Finance.JSON.createObjectNode();
        r.put("seq", seq); r.put("event_id", id); r.put("campaign", 0);
        r.put("event_ms", time); r.put("kind", kind); r.put("amount_minor", amount); r.put("sent_ms", 1);
        return r;
    }
    @Test void exactClassesAndStateSurviveSerialization() throws Exception {
        Finance.State state = new Finance.State();
        var a = row(0, "a", 2000000, "order", 100);
        assertEquals("accepted", Finance.apply(a, state, false).path("classification").asText());
        assertEquals("duplicate", Finance.apply(a, state, true).path("classification").asText());
        state = Finance.JSON.readValue(Finance.JSON.writeValueAsBytes(state), Finance.State.class);
        assertEquals("late", Finance.apply(row(2,"b",1100000,"refund",30),state,false).path("classification").asText());
        assertEquals("too_late", Finance.apply(row(3,"c",1099999,"order",1000),state,false).path("classification").asText());
        ObjectNode invalid = row(4,"d",2000000,"order",0); invalid.put("amount_minor",true);
        var result = Finance.apply(invalid,state,false);
        assertEquals("invalid",result.path("classification").asText());
        assertEquals(70,result.path("net_minor").asLong());
        assertEquals("accepted",Finance.apply(row(5,"e",1700000,"order",0),state,false).path("classification").asText());
    }
    @Test void topologyProducesOneExactRecordPerInput() {
        Properties p = new Properties(); p.put("application.id","eval-test");p.put("bootstrap.servers","unused:9092");
        try (TopologyTestDriver driver = new TopologyTestDriver(StreamsJob.topology("in","out"),p)) {
            var input = driver.createInputTopic("in",Serdes.String().serializer(),Serdes.String().serializer());
            var output = driver.createOutputTopic("out",Serdes.String().deserializer(),Serdes.String().deserializer());
            var a = row(0,"a",2000000,"order",100);
            input.pipeInput("0",a.toString()); a.put("seq",1);input.pipeInput("0",a.toString());
            var results = output.readValuesToList(); assertEquals(2,results.size());
            assertTrue(results.get(1).contains("duplicate"));assertTrue(results.get(1).contains("\"net_minor\":100"));
        }
    }
}
