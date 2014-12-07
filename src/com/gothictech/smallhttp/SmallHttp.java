package com.gothictech.smallhttp;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class SmallHttp extends Application<Config> {
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
    // nothing to do yet
  }

  @Override
  public void run(Config configuration, Environment environment) {
    final Services resource = new Services(
        configuration.getTemplate(),
        configuration.getDefaultName()
    );
    final ServicesHealthCheck healthCheck =
        new ServicesHealthCheck(configuration.getTemplate());
    environment.healthChecks().register("template", healthCheck);
    environment.jersey().register(resource);
  }

}
