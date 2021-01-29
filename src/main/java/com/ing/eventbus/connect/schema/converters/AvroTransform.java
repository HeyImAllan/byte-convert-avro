/* GNU GENERAL PUBLIC LICENSE */
package com.ing.eventbus.connect.schema.converters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import joptsimple.internal.Strings;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

// @SuppressWarnings("unused")
public class AvroTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Take an avro byte encoded message from a kafka topic, attach the schema, and send it forward. ";
    public static final ConfigDef CONFIG_DEF;
    public static final String SCHEMA_CAPACITY_CONFIG_DOC = "The maximum amount of schemas to be stored for each Schema Registry client.";
    public static final Integer SCHEMA_CAPACITY_CONFIG_DEFAULT = 100;
    public static final String SRC_PREAMBLE = "For source consumer's schema registry, ";
    public static final String SRC_SCHEMA_REGISTRY_CONFIG_DOC = "A list of addresses for the Schema Registry to copy from. The consumer's Schema Registry.";
    public static final String INCLUDE_HEADERS_CONFIG_DOC = "Whether or not to preserve the Kafka Connect Record headers.";
    public static final Boolean INCLUDE_HEADERS_CONFIG_DEFAULT = true;
    public static final String AVRO_TOPICS_DOC = "A list with the avro topics using the format <topic-name>:<true|false> where the second param " +
            "sets if the key is also serialized with avro or not";
    private static final Logger log = LoggerFactory.getLogger(AvroTransform.class);
    private static final byte MAGIC_BYTE = (byte) 0x0;
    // wire-format is magic byte + an integer, then data
    //private static final short WIRE_FORMAT_PREFIX_LENGTH = 1 + (Integer.SIZE / Byte.SIZE);

    static {
        CONFIG_DEF = (new ConfigDef())
                .define(ConfigName.SRC_SCHEMA_REGISTRY_URL, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, SRC_SCHEMA_REGISTRY_CONFIG_DOC)
                .define(ConfigName.SCHEMA_CAPACITY, ConfigDef.Type.INT, SCHEMA_CAPACITY_CONFIG_DEFAULT, ConfigDef.Importance.LOW, SCHEMA_CAPACITY_CONFIG_DOC)
                .define(ConfigName.INCLUDE_HEADERS, ConfigDef.Type.BOOLEAN, INCLUDE_HEADERS_CONFIG_DEFAULT, ConfigDef.Importance.LOW, INCLUDE_HEADERS_CONFIG_DOC)
                .define(ConfigName.AVRO_TOPICS, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, AVRO_TOPICS_DOC)
        ;
        // TODO: Other properties might be useful, e.g. the Subject Strategies
    }

    private CachedSchemaRegistryClient sourceSchemaRegistryClient;
    private SubjectNameStrategy<org.apache.avro.Schema> subjectNameStrategy;
    private boolean includeHeaders;

    // caches from the source registry to prevent abusing the schema registry.
    private Cache<Integer, org.apache.avro.Schema> schemaCache;
    private Map<String, Boolean> avroTopicMap;

    public AvroTransform() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> props) {
        log.info("AvroTransform - configure AvroTransform - START");
        props.entrySet().stream().forEach(e -> log.info(e.getKey() + "=" + e.getValue()));

        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        List<String> sourceUrls = config.getList(ConfigName.SRC_SCHEMA_REGISTRY_URL);
        final Map<String, String> sourceProps = new HashMap<>();

        Integer schemaCapacity = config.getInt(ConfigName.SCHEMA_CAPACITY);

        List<String> tempAvroList = config.getList(ConfigName.AVRO_TOPICS);
        this.avroTopicMap = validateAndParseAvroTopics(tempAvroList);
        String mapAsString = avroTopicMap.keySet().stream()
                .map(key -> key + "=" + avroTopicMap.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
        log.info("SchemaRegistryTransfer - loaded avro topic map:{}", mapAsString);

        this.schemaCache = new SynchronizedCache<>(new LRUCache<>(schemaCapacity));
        this.sourceSchemaRegistryClient = new CachedSchemaRegistryClient(sourceUrls, schemaCapacity, sourceProps);
        this.includeHeaders = config.getBoolean(ConfigName.INCLUDE_HEADERS);

        log.info("AvroTransform - use default io.confluent.kafka.serializers.subject.TopicNameStrategy for managing schema");
        this.subjectNameStrategy = new TopicNameStrategy();
        log.info("AvroTransform - configure AvroTransform - END");
    }

    @Override
    public R apply(R r) {
        final String topic = r.topic();
        log.info("AvroTransform - process record: {}", r.toString());

        // Transcribe the key's schema id
        final Object key = r.key();
        final Schema keySchema = r.keySchema();

        log.info("AvroTransform - apply for {} / {}", r.topic(), r.kafkaPartition());
        // Create new key.
        Object updatedKey = key;
        if (processKeys(topic)) {
            if (ConnectSchemaUtil.isBytesSchema(keySchema) || key instanceof byte[]) {
                if (key == null) {
                    log.info("AvroTransform - Passing through null record key.");
                } else {
                    log.info("AvroTransform - process key: {}", new String((byte[]) key));
                    byte[] keyAsBytes = (byte[]) key;
                    int keyByteLength = keyAsBytes.length;
                    if (keyByteLength <= 5) {
                        throw new SerializationException("AvroTransform - Unexpected byte[] length " + keyByteLength + " for Avro record key.");
                    }
                    ByteBuffer b = ByteBuffer.wrap(keyAsBytes);
                    // Do something to add avro to key value.
                    
                    org.apache.avro.Schema keyAvroSchema = getSchema(b, topic, true);
                    try {
                        updatedKey = rewriteToAvroJsonFormat(b, keyAvroSchema);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            } else {
                throw new ConnectException("AvroTransform - Transform failed. Record key does not have a byte[] schema.");
            }
        }
    

        // Transcribe the value's schema id
        final Object value = r.value();
        final Schema valueSchema = r.valueSchema();
        
        // Create new value
        Object updatedValue = value;

        if (ConnectSchemaUtil.isBytesSchema(valueSchema) || value instanceof byte[]) {
            if (value == null) {
                log.info("AvroTransform - Passing through null record value");
            } else {
                log.info("AvroTransform - process value: {}", new String((byte[]) value));
                byte[] valueAsBytes = (byte[]) value;
                int valueByteLength = valueAsBytes.length;
                if (valueByteLength <= 5) {
                    throw new SerializationException("AvroTransform - Unexpected byte[] length " + valueByteLength + " for Avro record value.");
                }
                ByteBuffer b = ByteBuffer.wrap(valueAsBytes);
                org.apache.avro.Schema valueAvroSchema = getSchema(b, topic, false);
                try {
                    updatedValue = rewriteToAvroJsonFormat(b, valueAvroSchema);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } else {
            throw new ConnectException("AvroTransform - Transform failed. Record value does not have a byte[] schema.");
        }


        return includeHeaders ?
                r.newRecord(topic, r.kafkaPartition(),
                        keySchema, updatedKey,
                        valueSchema, updatedValue,
                        r.timestamp(),
                        r.headers())
                :
                r.newRecord(topic, r.kafkaPartition(),
                        keySchema, updatedKey,
                        valueSchema, updatedValue,
                        r.timestamp());
    }

    private boolean processKeys(String topic) {
        return avroTopicMap.containsKey(topic) ? avroTopicMap.get(topic) : false;
    }

    

    private byte[] rewriteToAvroJsonFormat(ByteBuffer value, org.apache.avro.Schema valueAvroSchema)
            throws IOException {
        //Instantiating the Schema.Parser class.
        org.apache.avro.Schema schema = valueAvroSchema;
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(value.array(), null);
        GenericRecord emp = datumReader.read(null, decoder);
        log.info(emp.toString());
        return value.array();
    }

    protected org.apache.avro.Schema getSchema(ByteBuffer buffer, String topic, boolean isKey) {
        org.apache.avro.Schema schema;
        if (buffer.get() == MAGIC_BYTE) {
            int sourceSchemaId = buffer.getInt();

            schema = schemaCache.get(sourceSchemaId);
            if (schema != null) {
                log.info("AvroTransform - Schema id {} has been seen before. Not retrieving the schema again.");
            } else { // cache miss
                log.info("AvroTransform - Schema id {} has not been seen before", sourceSchemaId);
                try {
                    log.info("AvroTransform - Looking up schema id {} in source registry", sourceSchemaId);
                    // Can't do getBySubjectAndId because that requires a Schema object for the strategy
                    schema = sourceSchemaRegistryClient.getById(sourceSchemaId);
                } catch (IOException | RestClientException e) {
                    log.error(String.format("AvroTransform - Unable to fetch source schema for id %d.", sourceSchemaId), e);
                    throw new ConnectException(e);
                }
            }
        } else {
            throw new SerializationException("AvroTransform - Unknown magic byte!");
        }
        return schema;
    }

    @Override
    public void close() {
        this.sourceSchemaRegistryClient = null;
    }

    private Map<String, Boolean> validateAndParseAvroTopics(List<String> avroTopicList) {
        if (null == avroTopicList || avroTopicList.isEmpty()) {
            throw new ConnectException("There are no avro topics mentioned. Do not use this plugin if the avro schema replication is not needed.");
        }
        Map<String, Boolean> avroTopicKeyMap = new HashMap<>();
        boolean invalid = false;
        for (String topicWithKeyFlag : avroTopicList) {
            if (false == topicWithKeyFlag.contains(":")) {
                invalid = true;
                break;
            }
            String[] split = topicWithKeyFlag.split(":");
            if (split.length != 2) {
                invalid = true;
                break;
            }
            if (Strings.isNullOrEmpty(split[0]) || Strings.isNullOrEmpty(split[1])) {
                invalid = true;
                break;
            } else {
                avroTopicKeyMap.put(split[0], Boolean.valueOf(split[1]));
            }
        }
        if (invalid) {
            throw new ConnectException("Property " + ConfigName.AVRO_TOPICS + " does not respect the format: " +
                    "<topic-name>:<false|true>,<topic-name>:<false|true>,.....");
        }
        return avroTopicKeyMap;
    }

    interface ConfigName {
        String SRC_SCHEMA_REGISTRY_URL = "src." + AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
        String DEST_SCHEMA_REGISTRY_URL = "dest." + AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
        String SCHEMA_CAPACITY = "schema.capacity";
        String INCLUDE_HEADERS = "include.headers";
        String AVRO_TOPICS = "avro.topics";
    }
}
