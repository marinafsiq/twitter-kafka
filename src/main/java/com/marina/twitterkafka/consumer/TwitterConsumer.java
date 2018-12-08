package com.marina.twitterkafka.consumer;

import com.google.common.collect.Lists;
import com.marina.twitterkafka.utils.KafkaConstants;
import com.marina.twitterkafka.utils.TwitterConstants;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterConsumer {
    Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());

    public void run(){

        //creating Twitter client and connecting to it.
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        List<String> termsToSearch = Lists.newArrayList("healthylifestyle", "yoga", "run", "running", "healthyfood", "swimming", "meditation");
        HashMap<String, String> keysAndTokens = getTwitterKeysAndTokens("//home//marina//IdeaProjects//twitter-kafka//twitterpass.log");
        Client client = getTwitterClient(msgQueue, termsToSearch, keysAndTokens);
        client.connect();

        // getting Kafka Producer
        KafkaProducer<String, String> producer = getProducer();



        //Reading twitter data (from msgQueue) and placing them into kafka
        while(!client.isDone()){
            try{
                String msg = msgQueue.take();
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "\n\n-------> NOVA MENSAGEM!!!!\n" + msg);
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null)
                            logger.error("It iwas not possible to send records to the topic.", e);
                    }
                });
                producer.flush();
            }catch (Exception e){
                logger.error("It was not possible to get messages from Twitter client. Closing client connection.");
                client.stop();
                producer.close();
                e.printStackTrace();
            }
        }


        client.stop();
        producer.close();
    }

    private KafkaProducer<String, String> getProducer(){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, KafkaConstants.ACKS_ALL);

        //to make a safe producer - Idempotence
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        //high throughput producer (at the expense o a bit of latency and CPU usage)
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConstants.COMPRESSION_TYPE_SNAPPY);
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32KB batch size


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        return producer;
    }



    private Client getTwitterClient(BlockingQueue<String> msgQueue, List<String> termsToSerch, HashMap<String, String> keysAndTokens){

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        endpoint.trackTerms(termsToSerch);

        Authentication authentication = new OAuth1(keysAndTokens.get(TwitterConstants.CONSUMER_KEYS),
                keysAndTokens.get(TwitterConstants.CONSUMER_SECRET),
                keysAndTokens.get(TwitterConstants.TOKEN),
                keysAndTokens.get(TwitterConstants.TOKEN_SECRET));

        //create a client
        ClientBuilder clientBuilder = new ClientBuilder()
                .name("Twitter-Client")
                .hosts(hosts)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return clientBuilder.build();
    }

    private HashMap<String, String> getTwitterKeysAndTokens(String fileName){
        logger.info("getting Twitter Keys And Tokens");
        HashMap<String, String> hashMap = new HashMap<String, String>();

        File file = new File(fileName);
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = bufferedReader.readLine()) != null){
                String[] splittedLine = line.split(":");
                hashMap.put(splittedLine[0].trim(), splittedLine[1].trim());
            }

        }catch (Exception e){
            logger.error("It was not possible to read Twitter Password file.");
            e.printStackTrace();

        }
        return hashMap;
    }

}
