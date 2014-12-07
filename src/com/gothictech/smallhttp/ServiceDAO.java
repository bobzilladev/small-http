package com.gothictech.smallhttp;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;



public final class ServiceDAO {

    private final AmazonDynamoDBClient client;
    private static final String tableName = "services";

    public ServiceDAO(final AmazonDynamoDBClient client) {
        this.client = client;
    }

    public void createTable() {
        // Provide the initial provisioned throughput values as Java long data types
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
            .withReadCapacityUnits(5L)
            .withWriteCapacityUnits(6L);
        CreateTableRequest request = new CreateTableRequest()
            .withTableName(tableName)
            .withProvisionedThroughput(provisionedThroughput);
        
        ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("InsertTime").withAttributeType("N"));
        request.setAttributeDefinitions(attributeDefinitions);
        
        ArrayList<KeySchemaElement> tableKeySchema = new ArrayList<KeySchemaElement>();
        tableKeySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));
        request.setKeySchema(tableKeySchema);
  
        client.createTable(request);
        
        waitForTableToBecomeAvailable(tableName); 

        getTableInformation();
    }

    private void waitForTableToBecomeAvailable(String tableName) {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            DescribeTableRequest request = new DescribeTableRequest()
                    .withTableName(tableName);
            TableDescription tableDescription = client.describeTable(
                    request).getTable();
            String tableStatus = tableDescription.getTableStatus();
            System.out.println("  - current state: " + tableStatus);
            if (tableStatus.equals(TableStatus.ACTIVE.toString()))
                return;
            try { Thread.sleep(1000 * 20); } catch (Exception e) { }
        }
        throw new RuntimeException("Table " + tableName + " never went active");
    }

    public void deleteTable() {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
            .withTableName(tableName);
        DeleteTableResult result = client.deleteTable(deleteTableRequest);
        waitForTableToBeDeleted(tableName);  
    }

    private void waitForTableToBeDeleted(String tableName) {
        System.out.println("Waiting for " + tableName + " while status DELETING...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = client.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (ResourceNotFoundException e) {
                System.out.println("Table " + tableName + " is not found. It was deleted.");
                return;
            }
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
        }
        throw new RuntimeException("Table " + tableName + " was never deleted");
    }

    public void insert(final String id) {
        final Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("Id", new AttributeValue().withN(id));
        item.put("InsertTime", new AttributeValue().withN("" + System.currentTimeMillis()));
            
        PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
        client.putItem(itemRequest);
        item.clear();
    }

    public void update(final String id) {
        System.out.println("update: todo");
    }

    public void delete(final String id) {
        HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue> ();
        key.put("Id", new AttributeValue().withN(id));
        
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
            .withTableName(tableName)
            .withKey(key);
        
        DeleteItemResult deleteItemResult = client.deleteItem(deleteItemRequest);
        System.out.println("deleteItemResult: " + deleteItemResult);
    }

    public void get(final String id) {
        System.out.println("get: todo");

        HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("Id", new AttributeValue().withN("101"));

        GetItemRequest getItemRequest = new GetItemRequest()
            .withTableName(tableName)
            .withKey(key);

        GetItemResult result = client.getItem(getItemRequest);
        Map<String, AttributeValue> map = result.getItem();
        System.out.println("map: " + map);
    }

    private void getTableInformation() {
        TableDescription tableDescription = client.describeTable(
                new DescribeTableRequest().withTableName(tableName)).getTable();
        System.out.format("Name: %s:\n" +
                "Status: %s \n" + 
                "Provisioned Throughput (read capacity units/sec): %d \n" +
                "Provisioned Throughput (write capacity units/sec): %d \n",
                tableDescription.getTableName(),
                tableDescription.getTableStatus(),
                tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
    }
}
