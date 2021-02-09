# byte-convert-avro

Contains a kafka transforms plugin that converts confluent avro bytes to schema included JSON format. It is meant to be used in combination with [MirrorMaker2](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0). I couldn't find any public resource that is capable of providing this functionality which was required for our project, so we decided to build our own.

Thanks to the ING Dora team for providing me with a sample code and repository to start with. This really helped me get a jumpstart on this project.
Turns out this sample code was taken from [https://github.com/OneCricketeer/schema-registry-transfer-smt](https://github.com/OneCricketeer/schema-registry-transfer-smt), many thanks to OneCricketeer!

## Installation

1. Configure MirrorMaker

    Make sure that the plugin path is included in the CLASS\_PATH variable that is used for MirrorMaker2.
    For example:
    <span class="colour" style="color:rgb(212, 212, 212)">CLASSPATH="/your/path/kafkaconnect-mirrormaker/plugins/\*"</span>

    <span class="colour" style="color:rgb(106, 153, 85)"># Requires that records are entirely byte-arrays. These can go in the worker or connector configuration.</span>
    <span class="colour" style="color:rgb(86, 156, 214)">key.converter</span><span class="colour" style="color:rgb(212, 212, 212)">=org.apache.kafka.connect.converters.ByteArrayConverter</span>
    <span class="colour" style="color:rgb(86, 156, 214)">value.converter</span><span class="colour" style="color:rgb(212, 212, 212)">=org.apache.kafka.connect.converters.ByteArrayConverter</span>
    <span class="colour" style="color:rgb(106, 153, 85)">#Setup the SMT </span>
    <span class="colour" style="color:rgb(212, 212, 212)">{{ source\_cluster\_alias }}->{{ destination\_cluster\_alias }}.</span><span class="colour" style="color:rgb(86, 156, 214)">transforms</span><span class="colour" style="color:rgb(212, 212, 212)">=AvroSchemaTransfer</span>
    <span class="colour" style="color:rgb(106, 153, 85)">#Implementation of the connect Transformation</span>
    <span class="colour" style="color:rgb(212, 212, 212)">{{ source\_cluster\_alias }}->{{ destination\_cluster\_alias }}.</span><span class="colour" style="color:rgb(86, 156, 214)">transforms.AvroSchemaTransfer.type</span><span class="colour" style="color:rgb(212, 212, 212)">=com.ing.eventbus.connect.schema.converters.AvroTransform</span>
    <span class="colour" style="color:rgb(106, 153, 85)">#Source and target Schema Registry urls</span>
    <span class="colour" style="color:rgb(212, 212, 212)">{{ source\_cluster\_alias }}->{{ destination\_cluster\_alias }}.</span><span class="colour" style="color:rgb(86, 156, 214)">transforms.AvroSchemaTransfer.src.schema.registry.url</span><span class="colour" style="color:rgb(212, 212, 212)">=[http://schema-reg-url:8081](http://kafka-tst-1.europe.intranet:8081)</span>
    <span class="colour" style="color:rgb(106, 153, 85)"># List of avro topics for which this SMT will be applied. The boolean flag sets if the key also has an avro schema</span>
    <span class="colour" style="color:rgb(212, 212, 212)">{{ source\_cluster\_alias }}->{{ destination\_cluster\_alias }}.</span><span class="colour" style="color:rgb(86, 156, 214)">transforms.AvroSchemaTransfer.avro.topics</span><span class="colour" style="color:rgb(212, 212, 212)">=</span><span class="colour" style="color:rgb(206, 145, 120)">"test-topic:false"</span>
2. Build the project

    ```
    mvn clean package
    ```

3. Copy the JAR from target to all Kafka Connect workers under a directory set by CLASS_PATH.

4. Restart MirrorMaker2

