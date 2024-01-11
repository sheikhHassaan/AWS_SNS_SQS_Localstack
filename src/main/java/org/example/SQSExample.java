package org.example;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.*;
import software.amazon.awssdk.services.sns.*;
import software.amazon.awssdk.services.sqs.model.*;
import java.net.URI;

public class SQSExample
{
    public static void main(String[] args) {

        SqsClient sqsClient = SqsClient.builder().credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("hassaan", "Helloworld")))
                                .region(Region.US_WEST_2)
                                .endpointOverride(URI.create("http://localhost:4566"))
                                .build();

        CreateQueueResponse queue1Response = createQueue(sqsClient, "queue1");

        String queue1Url = queue1Response.queueUrl().toString();
        CreateQueueResponse queue2Response = createQueue(sqsClient, "queue2");
        String queue2Url = queue2Response.queueUrl().toString();

        SendMessageResponse sendMessageResponse1 = sendMessageToQueue(sqsClient, queue1Url, "Hello, queue1");
        System.out.println("sendMessageResponse1: "+sendMessageResponse1);

        SendMessageResponse sendMessageResponse2 = sendMessageToQueue(sqsClient, queue2Url, "Hello, queue2");
        System.out.println("sendMessageResponse2: "+sendMessageResponse2);

        listQueues(sqsClient);

        System.out.println("Messages of Queue1: -\n");
        listMessages(sqsClient, queue1Url);

        System.out.println("Messages of Queue2: -\n");
        listMessages(sqsClient, queue2Url);

        ReceiveMessageResponse receiveMessage1Response = receiveMessageFromQueue(sqsClient, queue1Url);
        System.out.println("receiveMessage1Response: "+receiveMessage1Response);

        ReceiveMessageResponse receiveMessage2Response = receiveMessageFromQueue(sqsClient, queue2Url);
        System.out.println("receiveMessage2Response: "+receiveMessage2Response);

        DeleteQueueResponse deleteQueue1Response = deleteQueue(sqsClient, queue1Url);
        System.out.println(deleteQueue1Response.toString());

        DeleteQueueResponse deleteQueue2Response = deleteQueue(sqsClient, queue2Url);
        System.out.println(deleteQueue2Response.toString());
    }

    public static CreateQueueResponse createQueue(SqsClient sqsClient, String queueName) {
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
        return createQueueResponse;
    }

    public static SendMessageResponse sendMessageToQueue(SqsClient sqsClient, String queueUrl, String messageBody) {
        // aws sqs send-message --queue-url <your-queue-url> --message-body "Hello, SQS!"
        SendMessageRequest sendMessageRequest;
        SendMessageResponse sendMessageResponse = null;
        try {
            sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageBody)
                    .build();

            sendMessageResponse = sqsClient.sendMessage(sendMessageRequest);
        } catch (Exception e) {
            System.out.println("Exception thrown. "+e.getMessage());
        }
        return sendMessageResponse;
    }

    public static ReceiveMessageResponse receiveMessageFromQueue(SqsClient sqsClient, String queueUrl) {
        // aws sqs receive-message --queue-url <your-queue-url>
        ReceiveMessageRequest receiveMessageRequest;
        ReceiveMessageResponse receiveMessageResponse = null;
        try {
            receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)    // Setting maximum number of messages at a time to get.
                    .build();

            receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);

        } catch (Exception e) {
            System.out.println("Exception thrown. "+e.getMessage());
        }
        return receiveMessageResponse;
    }
    public static DeleteQueueResponse deleteQueue(SqsClient sqsClient, String endpoint) {
        // aws sqs delete-queue --queue-url <your-queue-url>
        DeleteQueueRequest deleteQueueRequest;
        DeleteQueueResponse deleteQueueResponse = null;
        try {
            deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(endpoint)
                    .build();

            deleteQueueResponse = sqsClient.deleteQueue(deleteQueueRequest);
        } catch (QueueDoesNotExistException e) {
            System.out.println("The queue you are trying to delete doesn't exist.");
        }
        catch (Exception e) {
            System.out.println("Exception thrown. "+e.getMessage());
        }
        return deleteQueueResponse;
    }

    public static void listMessages(SqsClient sqsClient, String queueUrl) {
        try {
            ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(builder ->
                    builder.queueUrl(queueUrl));

            for (Message message : receiveMessageResponse.messages()) {
                System.out.println("Message ID: " + message.messageId());
                System.out.println("Message Body: " + message.body());
                System.out.println("Receipt Handle: " + message.receiptHandle());
                System.out.println();
            }
        } catch (Exception e) {
            System.err.println("Error listing messages: " + e.getMessage());
        }
    }

    public static void listQueues(SqsClient sqsClient) {
        try {
            ListQueuesResponse listQueuesResponse = sqsClient.listQueues();
            listQueuesResponse.queueUrls().forEach(System.out::println);
        } catch (SqsException e) {
            System.err.println("Error listing queues: " + e.getMessage());
        }
    }

}
