package fr.erdf.distribution.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaClient implements Runnable, AutoCloseable {

    private static final Schema OFFSET_KEY_SCHEMA_V0 = new Schema(
            new Field("group", Type.STRING),
            new Field("topic", Type.STRING),
            new Field("partition", Type.INT32)
    );

    private static final Schema OFFSET_KEY_SCHEMA_V3 = new Schema(
            new Field("group", Type.STRING)
    );

    private static final Schema OFFSET_VALUE_SCHEMA_V1 = new Schema(
            new Field("offset", Type.INT64),
            new Field("metadata", Type.STRING),
            new Field("timestamp", Type.INT64)
    );

    private static final Schema OFFSET_VALUE_SCHEMA_V0 = new Schema(
            new Field("offset", Type.INT64),
            new Field("metadata", Type.STRING),
            new Field("commit_timestamp", Type.INT64),
            new Field("expire_timestamp", Type.INT64)
    );

    private static final Schema GROUP_VALUE_SCHEMA_V0 = new Schema(
            new Field("protocol_type", Type.STRING), // protocole type key
            new Field("generation", Type.INT32), // Generation key
            new Field("protocol", Type.NULLABLE_STRING), // protocole key
            new Field("leader", Type.NULLABLE_STRING),
            new Field("members", new ArrayOf(
                    new Schema(
                            new Field("member_id", Type.STRING),
                            new Field("client_id", Type.STRING),
                            new Field("host", Type.STRING),
                            new Field("session_timeout", Type.INT32),
                            new Field("subscription", Type.BYTES),
                            new Field("assignement", Type.BYTES)
                    )
            ))
    );

    private static final Schema GROUP_VALUE_SCHEMA_V1 = new Schema(
            new Field("protocol_key_type", Type.STRING), // protocole type key
            new Field("generation_key", Type.INT32), // Generation key
            new Field("protocol_key", Type.NULLABLE_STRING), // protocole key
            new Field("leader", Type.NULLABLE_STRING),
            new Field("members", new ArrayOf(
                    new Schema(
                            new Field("member_id", Type.STRING),
                            new Field("client_id", Type.STRING),
                            new Field("host", Type.STRING),
                            new Field("rebalance_timeout", Type.INT32),
                            new Field("session_timeout", Type.INT32),
                            new Field("subscription", Type.BYTES),
                            new Field("assignement", Type.BYTES)
                    )
            ))
    );


    private volatile boolean shutdown = false;

    private KafkaConsumer<ByteBuffer, ByteBuffer> consumer;
    private long consumerTimeOut;

    private void init(){
        consumerTimeOut = Cfg.getLong("consumer.poll.timeout", TimeUnit.SECONDS.toMillis(30));

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Cfg.getString("brokers.list"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Cfg.getString("group.id"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, TimeUnit.SECONDS.toMillis(30));
        properties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, false);
        ByteBufferDeserializer keyDeserializer = new ByteBufferDeserializer();
        ByteBufferDeserializer valueDeserializer = new ByteBufferDeserializer();
        consumer = new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
    }


    public void run() {
        init();
        consumer.subscribe(Collections.singletonList("__consumer_offsets"));
        while (!shutdown){
            try {
                ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(consumerTimeOut);
                for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
                    consumeMessage(record);
                }
            } catch (InterruptException e){
                if(shutdown){
                    // log
                } else {
                    throw e;
                }
            }
        }
    }

    private void consumeMessage(ConsumerRecord<ByteBuffer, ByteBuffer> record) {
        ByteBuffer keyBuffer = record.key();
        Object key = null;
        if (keyBuffer != null) key = readKey(keyBuffer);
        if (key instanceof OffsetKey){

        } else if (key instanceof String) {

        }

        ByteBuffer value = record.value();
        if (value != null) readValue(value);
    }

    private void readValue(ByteBuffer buffer) {
        short version = buffer.getShort();
        Struct struct;
        switch (version){
            case 0:
                struct = OFFSET_VALUE_SCHEMA_V0.read(buffer);
                break;
            case 1:
                struct = OFFSET_VALUE_SCHEMA_V1.read(buffer);
                break;
        }
    }

    private Object readKey(ByteBuffer buffer) {
        short version = buffer.getShort();
        Struct struct;
        switch (version){
            case 0:
            case 1:
                struct = OFFSET_KEY_SCHEMA_V0.read(buffer);
                return new OffsetKey(version, struct.getString("group"), new TopicPartition(struct.getString("topic"), struct.getInt("partition" )));
            case 3:
                struct = OFFSET_KEY_SCHEMA_V3.read(buffer);
                return struct.getString("group");
            default:
                throw new IllegalStateException("Unsupported key version " + version);
        }
    }

    void readGroupMessageValue(String groupId, ByteBuffer buffer){
        if (buffer == null) { // tombstone
            null;
        } else {


            Schema schema = null;
            short version = buffer.getShort();
            switch (version){
                case 0:
                     schema = GROUP_VALUE_SCHEMA_V0; // leader key
                    // Array

                    break;
                case 1:
                    schema = GROUP_VALUE_SCHEMA_V1; // leader key
                    Type.STRING.read(buffer); // protocole type key
                    Type.INT32.read(buffer); // Generation key
                    Type.NULLABLE_STRING.read(buffer); // protocole key
                    Type.NULLABLE_STRING.read(buffer); // leader key

                    break;
            }
            val valueSchema = schemaForGroup(version)
            val value = valueSchema.read(buffer)

            if (version == 0 || version == 1) {
                val protocolType = value.get(PROTOCOL_TYPE_KEY).asInstanceOf[String]

                val memberMetadataArray = value.getArray(MEMBERS_KEY)
                val initialState = if (memberMetadataArray.isEmpty) Empty else Stable

                val group = new GroupMetadata(groupId, initialState)

                group.generationId = value.get(GENERATION_KEY).asInstanceOf[Int]
                group.leaderId = value.get(LEADER_KEY).asInstanceOf[String]
                group.protocol = value.get(PROTOCOL_KEY).asInstanceOf[String]

                memberMetadataArray.foreach { memberMetadataObj =>
                    val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
                    val memberId = memberMetadata.get(MEMBER_ID_KEY).asInstanceOf[String]
                    val clientId = memberMetadata.get(CLIENT_ID_KEY).asInstanceOf[String]
                    val clientHost = memberMetadata.get(CLIENT_HOST_KEY).asInstanceOf[String]
                    val sessionTimeout = memberMetadata.get(SESSION_TIMEOUT_KEY).asInstanceOf[Int]
                    val rebalanceTimeout = if (version == 0) sessionTimeout else memberMetadata.get(REBALANCE_TIMEOUT_KEY).asInstanceOf[Int]

                    val subscription = Utils.toArray(memberMetadata.get(SUBSCRIPTION_KEY).asInstanceOf[ByteBuffer])

                    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeout, sessionTimeout,
                            protocolType, List((group.protocol, subscription)))

                    member.assignment = Utils.toArray(memberMetadata.get(ASSIGNMENT_KEY).asInstanceOf[ByteBuffer])

                    group.add(member)
                }

                group
            } else {
                throw new IllegalStateException("Unknown group metadata message version")
            }
        }
    }

    public void close() throws Exception {
        if (consumer != null){
            consumer.close();
        }
    }

    private static final class OffsetKey {
        final short version;
        final String group;
        final TopicPartition topicPartition;

        OffsetKey(short version, String group, TopicPartition topicPartition) {
            this.version = version;
            this.group = group;
            this.topicPartition = topicPartition;
        }
    }
}