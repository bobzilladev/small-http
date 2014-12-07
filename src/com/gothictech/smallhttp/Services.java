package com.gothictech.smallhttp;

import com.google.common.base.Optional;
import com.codahale.metrics.annotation.Timed;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.atomic.AtomicLong;

@Path("/services")
@Produces(MediaType.APPLICATION_JSON)
public class Services{
    private final String template;
    private final String defaultName;
    private final AmazonDynamoDBClient client;
    private final AtomicLong counter;
    private final ServiceDAO serviceDao;

    public Services(String template, String defaultName, AmazonDynamoDBClient client) {
        this.template = template;
        this.defaultName = defaultName;
        this.client = client;
        this.counter = new AtomicLong();
        this.serviceDao = new ServiceDAO(client);
    }

    @GET
    @Timed
    public ServiceModel sayHello(@QueryParam("name") Optional<String> name,
            @QueryParam("id") Optional<String> id) {
        final String command = name.or(defaultName);
        switch(command) {
            case "createTable":
                serviceDao.createTable();
                break;
            case "deleteTable":
                serviceDao.deleteTable();
                break;
            case "insert":
                serviceDao.insert(id.or("42"));
                break;
            case "update":
                serviceDao.update(id.or("42"));
                break;
            case "delete":
                serviceDao.delete(id.or("42"));
                break;
            default:
                System.out.println("no command");
        }
        final String value = String.format(template, command);
        return new ServiceModel(counter.incrementAndGet(), value);
    }
}
