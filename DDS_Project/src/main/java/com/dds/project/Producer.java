package com.dds.project;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Producer {
    private static String AccessToken = "add your token";
    private static String AccessSecret = "add your access Secret";
    private static String ConsumerKey = "add your consumer key";
    private static String ConsumerSecret = "add consumer secret";

    private static HashMap authTokenMap = new HashMap();


    public static void main(String[] args) throws InterruptedException {
        final Logger logger = LoggerFactory.getLogger(Producer.class);
        // Create properties object for producer
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creat the producer
        final KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        ArrayList tweetList = new ArrayList();
        tweetList = mainTwitter();
        int key=1;
        for(Object ele : tweetList){
            ProducerRecord<String, String> record = new ProducerRecord<>("replica3Consumertopic","key_"+key,"\n######## POST"+key+" ########"+ele);
            ++key;
            //Send data -asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        logger.info("\nReceived meta data. \n"+"Topic:" + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() + "," +
                                "Offset: " + recordMetadata.offset() + "@ Timestamp: " + recordMetadata.timestamp()+ "\n");
                    }
                    else{
                        logger.error("Error has occured", e);
                    }
                }
            });
            TimeUnit.SECONDS.sleep(1);
        }
        //flush and close producer
        producer.flush();
        producer.close();
    }
    public static ArrayList mainTwitter() {
        // TODO Auto-generated method stub
        authTokenMap.put("AccessToken", AccessToken);
        authTokenMap.put("AccessSecret", AccessSecret);
        authTokenMap.put("ConsumerKey", ConsumerKey);
        authTokenMap.put("ConsumerSecret", ConsumerSecret);
        Twitter authObject = null;
        String topic = "#sonusood";
        ArrayList tweets = new ArrayList();
        try {
            authObject = authenticate(authTokenMap);
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println(e.getMessage());
        }
        try {
            tweets = getTwitterData(authObject, topic);
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println(e.getMessage());
        }
        System.out.println("Tweets related to " + topic + " : " + tweets);
        return tweets;
    }
    public static Twitter authenticate(HashMap<String, String> authTokenMap) {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setJSONStoreEnabled(true);
        cb.setOAuthConsumerKey(authTokenMap.get("ConsumerKey"));
        cb.setOAuthConsumerSecret(authTokenMap.get("ConsumerSecret"));
        cb.setOAuthAccessToken(authTokenMap.get("AccessToken"));
        cb.setOAuthAccessTokenSecret(authTokenMap.get("AccessSecret"));
        cb.setHttpConnectionTimeout(100000);

        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        return twitter;
    }
    public static ArrayList getTwitterData(Twitter authObject, String topic) {
        System.out.println("Inside tweet data method: "+authObject + " , "+topic);
        ArrayList tweetList = new ArrayList();
        try {
            Query query = new Query(topic);
            QueryResult result = null;
            do {
                result = authObject.search(query);
                List tweets = result.getTweets();
                for (Object tweet : tweets) {
                    Status tweet2 =(Status) tweet;
                    tweetList.add(tweet2.getText());
                }
            } while (result.hasNext() == true);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            System.out.println("Failed to search tweets: " + e.getMessage());
        }
        System.out.println("tweetsCount= "+tweetList.size());
        return tweetList;
    }
}
