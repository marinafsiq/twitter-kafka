package com.marina.twitterkafka;

import com.marina.twitterkafka.consumer.TwitterConsumer;
import com.marina.twitterkafka.utils.TwitterConstants;

public class Main {
    public static void main(String[] args) {
        TwitterConsumer twitterConsumer = new TwitterConsumer();
        twitterConsumer.fazTudoAgora();
    }
}
