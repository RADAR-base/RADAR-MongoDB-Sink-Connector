# RADAR-MongoDB-Sink-Connector
[![Build Status](https://travis-ci.org/RADAR-CNS/RADAR-MongoDB-Sink-Connector.svg?branch=master)](https://travis-ci.org/RADAR-CNS/RADAR-Commons)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/33b7a59ca6f946c2a168fa403e3e5644)](https://www.codacy.com/app/RADAR-CNS/RADAR-MongoDB-Sink-Connector?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-CNS/RADAR-MongoDB-Sink-Connector&amp;utm_campaign=Badge_Grade)

Implements the hot-storage support for RADAR-CNS platform using MongoDBSinkConnector

### To Run Radar-MongoDB-Connector

1. In addition to Zookeeper, Kafka-broker(s), Schema-registry and Rest-proxy, MongoDB should be running with required credentials
2. Load the `radar-mongodb-sink-connector-*.jar` to CLASSPATH
    
    ```ini
    export CLASSPATH=/path/to/radar-mongodb-sink-connector-0.1.jar
    ```
      
3. Configure MongoDB Connector properties.

    ```ini
    # Kafka consumer configuration
    name=radar-connector-mongodb-sink

    # Kafka connector configuration
    connector.class=org.radarcns.mongodb.MongoDbSinkConnector
    tasks.max=1

    # Topics that will be consumed
    topics=topics

    # MongoDB server
    mongo.host=localhost
    mongo.port=27017

    # MongoDB configuration
    mongo.username=
    mongo.password=
    mongo.database=

    # Collection name for putting data into the MongoDB database. The {$topic} token will be replaced
    # by the Kafka topic name.
    #mongo.collection.format={$topic}

    # Factory class to do the actual record conversion
    record.converter.class=org.radarcns.sink.mongodb.RecordConverterFactoryRadar
    ```
    For more details visit our [MongoDBConnector](https://github.com/RADAR-CNS/RADAR-MongoDbConnector) and [Kafka-Connect](http://docs.confluent.io/3.1.2/connect/quickstart.html)
   
4. Run the connector. To run the connector in `standalone mode` (on an enviornment where the Confluent platform is installed):
   
    ```shell
    connect-standalone /etc/schema-registry/connect-avro-standalone.properties path-to-your-mongodb-connector.properties
    ```   
   
## Contributing
Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).
If you want to contribute a feature or fix browse our [issues](https://github.com/RADAR-CNS/RADAR-MongoDB-Sink-Connector/issues), and please make a pull request.