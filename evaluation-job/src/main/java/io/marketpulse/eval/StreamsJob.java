package io.marketpulse.eval;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.*;

public final class StreamsJob {
    public static Topology topology(String input, String output) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("seen"),Serdes.String(),Serdes.Boolean()));
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("finance"),Serdes.String(),Serdes.String()));
        builder.stream(input,Consumed.with(Serdes.String(),Serdes.String()))
            .process(() -> new ContextualProcessor<String,String,String,String>() {
                KeyValueStore<String,Boolean> seen;
                KeyValueStore<String,String> states;
                @Override public void init(ProcessorContext<String,String> context) {
                    super.init(context);seen=context.getStateStore("seen");states=context.getStateStore("finance");
                }
                @Override public void process(org.apache.kafka.streams.processor.api.Record<String,String> record) {
                    try {
                        var row = Finance.JSON.readTree(record.value());
                        // Route by payload on both adapters; producer uses the same key.
                        String key = row.path("campaign").asText();
                        String old = states.get(key);
                        Finance.State state = old == null ? new Finance.State() : Finance.JSON.readValue(old,Finance.State.class);
                        String id = key+"|"+row.path("event_id").asText();
                        boolean duplicate = seen.get(id) != null;
                        var out = Finance.apply(row,state,duplicate);
                        if (Finance.valid(row)) seen.put(id,true);
                        states.put(key,Finance.JSON.writeValueAsString(state));
                        context().forward(record.withValue(out.toString()));
                    } catch (Exception e) { throw new IllegalStateException("invalid benchmark wire envelope",e); }
                }
            },"seen","finance").to(output,Produced.with(Serdes.String(),Serdes.String()));
        return builder.build();
    }
    public static void main(String[] args) {
        if(args.length!=4) throw new IllegalArgumentException("bootstrap input output application-id required");
        Properties p = new Properties();
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,args[0]);p.put(StreamsConfig.APPLICATION_ID_CONFIG,args[3]);
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.STATE_DIR_CONFIG,"/tmp/eval-streams");p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,1000);
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,1);p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,1);
        p.put("consumer.auto.offset.reset","earliest");p.put("producer.transaction.timeout.ms",60000);
        KafkaStreams streams = new KafkaStreams(topology(args[1],args[2]),p);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(10))));
        streams.start();
    }
}
