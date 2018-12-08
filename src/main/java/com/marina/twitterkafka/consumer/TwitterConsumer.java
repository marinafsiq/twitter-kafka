package com.marina.twitterkafka.consumer;

import com.google.common.collect.Lists;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterConsumer {
    Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());

    public void run(){

        //creating Twitter client and connecting to it.
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        List<String> termsToSearch = Lists.newArrayList("healthylifestyle", "yoga");
        HashMap<String, String> keysAndTokens = getTwitterKeysAndTokens("//home//marina//IdeaProjects//twitter-kafka//twitterpass.log");
        Client client = getTwitterClient(msgQueue, termsToSearch, keysAndTokens);
        client.connect();


        //connection information
        StringBuilder stb = new StringBuilder();
        int counter = 0;
        //getting the data
        while(!client.isDone()){
            try{
                String str = msgQueue.take();
                stb.append(str);
                stb.append("\n");
                System.out.println(counter++);
                System.out.println(str);
            }catch (Exception e){
                logger.error("It was not able to get messages from Twitter client. Closing client connection.");
                client.stop();
                e.printStackTrace();
            }
        }

        String finalStr = stb.toString();
        System.out.printf(finalStr);

        client.stop();
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
