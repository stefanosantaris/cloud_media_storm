package edu.csd;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.queue.client.CloudQueue;
import com.microsoft.windowsazure.services.queue.client.CloudQueueClient;
import com.microsoft.windowsazure.services.queue.client.CloudQueueMessage;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableOperation;

public class CandidateIdsRetrieval {

	public static final String storageConnectionString = "UseDevelopmentStorage=true";
	private final static String candidateImageIDRetrievalQueue = "candidateidretrievalqueue";
	private final static String imageComparatorQueue = "imagecomparatorqueue";
	private static List<Integer> positionsList = new ArrayList<>();
	private static String datasetName;
	private static Double W;
	private static int N, descriptorId, position, range;
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
					N = (Integer) jsonObject.get("numOfImages");
					W = (Double) jsonObject.get("W");
					descriptorId = (int) jsonObject.get("descriptorId");
					position = (int) jsonObject.get("position");

					// retrieve the positions of the descriptor vectors to
					// identify the candidate descriptor vectors
					retrieveTheDescriptorVectorsPositions();
					
					//Calculate the W range
					range = (int) ((int) N * W);
					for(int i = (position - range); i < position; i++) {
						sendMessageToComparator(positionsList.get(i));
					}
					
					for(int i = (position + 1); i < (position + range + 1); i++) {
						sendMessageToComparator(positionsList.get(i));
					}
				}
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private static void sendMessageToComparator(int	candidateId) {
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
					.getQueueReference(imageComparatorQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();

			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("descriptorId", new Integer(descriptorId));
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

	private static void retrieveTheDescriptorVectorsPositions() {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		PositionListEntity entity = null;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();

			TableOperation retrieveEntity = TableOperation.retrieve(
					datasetName, datasetName, PositionListEntity.class);
			entity = tableClient.execute(datasetName + "positions",
					retrieveEntity).getResultAsType();
			String jsonPositionList = entity.getJsonPositionList();
			Object obj = JSONValue.parse(jsonPositionList);
			JSONArray array = (JSONArray) obj;
			for (int i = 0; i < array.size(); i++) {
				positionsList.add((int) array.get(i));
			}

		} catch (InvalidKeyException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
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
			queue = queueClient
					.getQueueReference(candidateImageIDRetrievalQueue);

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
