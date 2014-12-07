package com.gothictech.smallhttp;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class SmallHttp extends Application<Config> {

  private AmazonDynamoDBClient client;

  public static void main(String[] args) {
    try {
      new SmallHttp().run(args);
      // Thread.sleep(60000);
    }
    catch (Exception ex) {
      System.out.println("ex: " + ex);
    }
  }

  @Override
  public String getName() {
    return "small-http";
  }

  @Override
  public void initialize(Bootstrap<Config> bootstrap) {
    client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
    // client.setRegion(Region.getRegion(Regions.US_WEST_1));
    client.setEndpoint("http://localhost:8000");
  }

  @Override
  public void run(Config configuration, Environment environment) {
    final Services resource = new Services(
        configuration.getTemplate(),
        configuration.getDefaultName(),
        client
    );
    final ServicesHealthCheck healthCheck =
        new ServicesHealthCheck(configuration.getTemplate());
    environment.healthChecks().register("template", healthCheck);
    environment.jersey().register(resource);
  }

}
