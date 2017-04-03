package edu.uprm.cse.bigdata.ex5;

import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Created by omar on 4/3/17.
 */
public class LocalConfigurationBuilderExample {
    static Configuration obj = null;

    public static Configuration getConfigurationBuilded() {
        if (obj == null) {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey("*****")
                    .setOAuthConsumerSecret("*****")
                    .setOAuthAccessToken("*****")
                    .setOAuthAccessTokenSecret("*****");

            obj = cb.build();
        }

        return obj;
    }
}
