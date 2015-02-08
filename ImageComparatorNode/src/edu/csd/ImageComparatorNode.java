package edu.csd;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;




import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.queue.client.CloudQueue;
import com.microsoft.windowsazure.services.queue.client.CloudQueueClient;
import com.microsoft.windowsazure.services.queue.client.CloudQueueMessage;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableOperation;

public class ImageComparatorNode {

	public static final String storageConnectionString = "UseDevelopmentStorage=true";
	private final static String imageComparatorQueue = "imagecomparatorqueue";
	private final static String distanceCollectorQueue = "distancecollectorqueue";
	private static String datasetName;
	private static int range;
	private static CloudQueue queue;
	
	public static void main(String[] args) {
		initialize();

		while (true) {
			try {
				CloudQueueMessage message = queue.retrieveMessage();
				if (message != null) {
					queue.deleteMessage(message);
					String json = message.getMessageContentAsString();

					// parse json string
					Object obj = JSONValue.parse(json);
					JSONObject jsonObject = (JSONObject) obj;
					datasetName = (String) jsonObject.get("dataset");
					int descriptorId = (int) jsonObject.get("descriptorId");
					int candidateId = (int) jsonObject.get("candidateId");
					range = (int) jsonObject.get("range");
					
					DescriptorVectorEntity queryEntity = retrieveEntity(descriptorId);
					DescriptorVectorEntity candidateEntity = retrieveEntity(candidateId);
					double result = compareImages(queryEntity, candidateEntity);
					
					
					sendMessageToDistanceCollector(result, candidateId);
				}
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	private static void sendMessageToDistanceCollector(double result,
			int candidateId) {
		try {
			// Retrieve storage account from connection-string
			// (The storage connection string needs to be changed in case of
			// cloud infrastructure
			// is used instead of emulator)
			CloudStorageAccount storageAccount = CloudStorageAccount
					.parse(storageConnectionString);

			// Create the queue client
			CloudQueueClient queueClient = storageAccount
					.createCloudQueueClient();

			// Retrieve a reference to a queue
			CloudQueue queue = queueClient
					.getQueueReference(distanceCollectorQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();

			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("result", new Double(result));
			obj.put("candidateId", new Integer(candidateId));
			obj.put("range", new Integer(range));
			
			// Convert the list with the cardinality values to json string
			String jsonString = obj.toJSONString();

			// Send the Message
			CloudQueueMessage message = new CloudQueueMessage(jsonString);
			queue.addMessage(message);

		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static double compareImages(DescriptorVectorEntity queryEntity,
			DescriptorVectorEntity candidateEntity) {
		String queryVector = queryEntity.getVector();
		String[] splitQueryVector = queryVector.split(",");
		
		String candidateVector = candidateEntity.getVector();
		String[] splitCandidateVector = candidateVector.split(",");
		
		double sum = 0.0;
		for(int i = 0; i <splitQueryVector.length; i++) {
			Double queryValue = Double.parseDouble(splitQueryVector[i]);
			Double candidateValue = Double.parseDouble(splitCandidateVector[i]);
			sum += Math.pow(queryValue - candidateValue, 2);
		}
		double result = Math.sqrt(sum);
		return result;
	}

	private static DescriptorVectorEntity retrieveEntity(
			int id) {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		DescriptorVectorEntity entity = null;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();
			
			TableOperation operation = TableOperation.retrieve(datasetName, Integer.toString(id), DescriptorVectorEntity.class);
			entity = tableClient.execute(datasetName, operation).getResultAsType();

		} catch (InvalidKeyException | URISyntaxException | StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return entity;
	}
	
	
	private static void initialize() {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the queue client
			CloudQueueClient queueClient = storageAccount
					.createCloudQueueClient();

			// Retrieve a reference to a queue
			queue = queueClient
					.getQueueReference(imageComparatorQueue);

		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
