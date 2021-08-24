package com.kishore.kamal.kafkae2e.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class LocalKafkaProducer {

    private static final String TOPIC = "SecondTopic";

    public static void main(String[] args) {
//        fireAndForgetProducer();
//        synchronousSendProducer();
        asynchronousSendProducer();
    }

    private static void fireAndForgetProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(TOPIC, "key", "Value-2");
        producer.send(producerRecord);
        producer.close();

    }

    private static void synchronousSendProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(TOPIC, "key", "Value-2");
        try{
            producer.send(producerRecord);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            producer.close();
        }

    }

    private static void asynchronousSendProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(TOPIC, "key", "Value-2");
        producer.send(producerRecord, new CustomCallBack());
        producer.close();
    }

    private static class CustomCallBack implements Callback{

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if( null != e){
                System.out.println("Exception occurred while sending to kafka "+ e.getMessage());
            }else{
                System.out.println("successfully send in kafka "+ recordMetadata.offset() + recordMetadata.partition());
            }
        }
    }
}
