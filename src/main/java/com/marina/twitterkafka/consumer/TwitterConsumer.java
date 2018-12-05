package com.marina.twitterkafka.consumer;

import com.google.common.collect.Lists;
import com.marina.twitterkafka.utils.TwitterConstants;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterConsumer {
    public void fazTudoAgora(){

        //connection information
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);

        Hosts hosts = new HttpHosts(TwitterConstants.HOSTS);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("healthylifestyle", "api");
        endpoint.trackTerms(terms);

        Authentication authentication = new OAuth1("","","","");


        //create a client
        ClientBuilder clientBuilder = new ClientBuilder()
                .name("marina-teste-01")
                .hosts(hosts)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client client = clientBuilder.build();
        client.connect();

        StringBuilder stb = new StringBuilder();
        //getting the data
        while(!client.isDone()){
            try{
                String str = msgQueue.take();
                stb.append(str);
                stb.append("\n");
            }catch (Exception e){e.printStackTrace();}
        }

        String finalStr = stb.toString();
        System.out.printf(finalStr);

        client.stop();

    }
}
