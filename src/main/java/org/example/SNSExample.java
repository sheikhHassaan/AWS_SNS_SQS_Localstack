package org.example;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sns.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.URI;

public class SNSExample {
    public static void main(String[] args) {

        String orderPlacedTopic = "orderPlaced";

        SnsClient snsClient = SnsClient.builder()
                .region(Region.US_WEST_2)
                .credentialsProvider(StaticCredentialsProvider.create(
                                AwsBasicCredentials.create("hassaan", "Helloworld")))
                .endpointOverride(URI.create("http://localhost:4566"))
                .build();

        // note : Created topic
        String orderPlacedArn = createTopic(snsClient, orderPlacedTopic).topicArn();
        System.out.println("orderPlacedArn: "+orderPlacedArn+'\n');

        // note : Published message to topic
        String messageId = publishMessageToTopic(snsClient, orderPlacedArn, "Hello first message to the topic "+orderPlacedTopic);
        System.out.println("MessageId of published message: "+messageId+'\n');

        // extra : creating a queue
        SqsClient sqsClient = SqsClient.builder().credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("hassaan", "Helloworld"))).region(Region.US_WEST_2).endpointOverride(URI.create("http://localhost:4566")).build();
        String queueUrl = createQueue(sqsClient, "testQueue");
        System.out.println("Queue URL: "+ queueUrl+'\n');

        // note : subscribing a queue to a topic
        String subscriptionArn = subscribeQueueToTopic(sqsClient, snsClient, orderPlacedArn ,queueUrl);
        System.out.println("SubscriptionARN"+subscriptionArn+'\n');

    }

    public static CreateTopicResponse createTopic(SnsClient snsClient, String topicName) {
        CreateTopicRequest createTopicRequest;
        CreateTopicResponse createTopicResponse = null;
        try {
            createTopicRequest = CreateTopicRequest.builder()
                    .name(topicName)
                    .build();

            createTopicResponse = snsClient.createTopic(createTopicRequest);
        } catch (Exception e) {
            System.out.println("Something went wrong. "+ e.getMessage());
        }
        return createTopicResponse;
    }

    public static String publishMessageToTopic(SnsClient snsClient, String topicArn, String message) {
        PublishRequest publishRequest;
        PublishResponse publishResponse = null;
        try {
            publishRequest = PublishRequest.builder()
                    .message(message)
                    .topicArn(topicArn)
                    .build();

            publishResponse = snsClient.publish(publishRequest);
        } catch (Exception e) {
            System.out.println("Something went wrong. "+ e.getMessage());
        }
        return publishResponse.messageId();
    }
    public static String subscribeQueueToTopic(SqsClient sqsClient, SnsClient snsClient, String topicArn, String queueUrl) {
        SubscribeRequest subscribeRequest;
        SubscribeResponse subscribeResponse = null;
        try {
            subscribeRequest = SubscribeRequest.builder()
                    .protocol("sqs")
                    .topicArn(topicArn)
                    .endpoint(getQueueArn(sqsClient, queueUrl))
                    .build();

            subscribeResponse = snsClient.subscribe(subscribeRequest);
        } catch (Exception e) {
            System.out.println("Something went wrong. "+ e.getMessage());
        }
        return subscribeResponse.subscriptionArn();
    }

    private static String getQueueArn(SqsClient sqsClient, String queueUrl) {
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.valueOf("QueueArn"))
                .build();

        GetQueueAttributesResponse getQueueAttributesResponse = sqsClient.getQueueAttributes(getQueueAttributesRequest);
        return getQueueAttributesResponse.toString();
    }

    public static String createQueue(SqsClient sqsClient, String queueName) {
        //aws sqs create-queue --queue-name MyQueue
        CreateQueueRequest createQueueRequest;
        CreateQueueResponse createQueueResponse = null;
        try {
            createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            createQueueResponse = sqsClient.createQueue(createQueueRequest);
        } catch (QueueNameExistsException e) {
            System.out.println("A queue with this name already exists.");
        }
        catch (Exception e) {
            System.out.println("Exception thrown. "+e.getMessage());
        }
        return createQueueResponse.queueUrl();
    }
}
