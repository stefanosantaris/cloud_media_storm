package edu.csd;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.queue.client.CloudQueue;
import com.microsoft.windowsazure.services.queue.client.CloudQueueClient;
import com.microsoft.windowsazure.services.queue.client.CloudQueueMessage;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableOperation;

public class DVCExtractor {

	private static CloudQueue queue;
	public static final String storageConnectionString = "UseDevelopmentStorage=true";
	private final static String dvcExtractorQueue = "dvcextractorqueue";
	private final static String priorityIndexQueue = "dvcpriorityindexqueue";
	private static HashMap<Integer, HashSet<Double>> cardinalityMap;
	private static List<Integer> cardinalityValues;
	public static void main(String[] args) {
		cardinalityMap = new HashMap<>();
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
					String datasetName = (String) jsonObject.get("dataset");
					int N = (Integer) jsonObject.get("size");
					int startDimension = (Integer) jsonObject
							.get("startDimension");
					int stopDimension = (Integer) jsonObject
							.get("stoDimension");

					calculateCardinalityValues(datasetName, N, startDimension,
							stopDimension);
					
					sendResultToPriorityIndex();
				}
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void sendResultToPriorityIndex() {
		try {
			//Retrieve storage account from connection-string 
			//(The storage connection string needs to be changed in case of cloud infrastructure
			//is used instead of emulator)
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			
			// Create the queue client
			CloudQueueClient queueClient = storageAccount.createCloudQueueClient();
		
			// Retrieve a reference to a queue
			CloudQueue queue = queueClient.getQueueReference(priorityIndexQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();
			
			//Convert the list with the cardinality values to json string
			String jsonString = JSONValue.toJSONString(cardinalityValues);
			
			//Send the Message
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

	private static void calculateCardinalityValues(String datasetName, int N,
			int startDimension, int stopDimension) {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();
			
			//contains the dimension of the descriptor vector
			int D = 0;

			for (int i = 1; i <= N; i++) {
				TableOperation retrievedEntity = TableOperation.retrieve(
						datasetName, Integer.toString(i),
						DescriptorVectorEntity.class);

				// Submit the operation to the table service and get the
				// specific
				// entity.
				DescriptorVectorEntity specificEntity = tableClient.execute(
						datasetName, retrievedEntity).getResultAsType();
				
				String descriptorVector = specificEntity.getVector();
				//Split the descriptor vector to process the specific dimensions
				String[] descriptors = descriptorVector.split(",");
				D = descriptors.length;
				for(int j = startDimension; j < stopDimension; j++) {
					double value = Double.parseDouble(descriptors[j]);
					if(!cardinalityMap.containsKey(j)) {
						HashSet<Double> dimensionCardinalityValue = new HashSet<>();
						dimensionCardinalityValue.add(value);
						cardinalityMap.put(j, dimensionCardinalityValue);
					} else {
						if(!cardinalityMap.get(j).contains(value)) {
							cardinalityMap.get(j).add(value);
						}
					}
				}
			}
			
			cardinalityValues = new ArrayList<>();
			int cardinalityValueScore = 0;
			for(int i = 0; i < D; i++) {
				if(cardinalityMap.containsKey(i)) {
					cardinalityValueScore = cardinalityMap.get(i).size();
				} else {
					cardinalityValueScore = 0;
				}
				cardinalityValues.add(cardinalityValueScore);
			}
		} catch (InvalidKeyException | URISyntaxException | StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
			queue = queueClient.getQueueReference(dvcExtractorQueue);

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
