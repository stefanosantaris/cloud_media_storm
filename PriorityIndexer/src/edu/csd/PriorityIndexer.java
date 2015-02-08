package edu.csd;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
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

public class PriorityIndexer {

	private static CloudQueue schedulerQueue, dvcQueue;
	public static final String storageConnectionString = "UseDevelopmentStorage=true";
	private final static String priorityIndexQueue = "priorityindexqueue";
	private final static String dvcPriorityIndexQueue = "dvcpriorityindexqueue";
	private final static String imageSorterNodesQueue = "imagesorterqueue";
	private final static String globalImageSorterQueue = "globalimagesorterqueue";
	private static List<List<Integer>> cardinalityValuesList;
	private static List<PriorityIndexValue> priorityIndex;
	private static String datasetName;
	private final static String priorityIndexTable = "priorityindex";
	private static int numOfImages, numberOfImageSorterNodes = 2;

	public static void main(String[] args) {
		// initialize the priorityindexqueue which retrieves the message from
		// the scheduler
		initialize(true);

		while (true) {
			try {
				CloudQueueMessage message = schedulerQueue.retrieveMessage();
				if (message != null) {
					schedulerQueue.deleteMessage(message);
					String json = message.getMessageContentAsString();

					// parse json string
					Object obj = JSONValue.parse(json);
					JSONObject jsonObject = (JSONObject) obj;
					datasetName = (String) jsonObject.get("dataset");
					int numOfC = (Integer) jsonObject.get("numOfC");
					numOfImages = (Integer) jsonObject.get("numOfImages");

					cardinalityValuesList = new ArrayList<>();
					// initialize the dvcpriorityindexqueue which retrieves the
					// L^{(m)}
					initialize(false);

					while (cardinalityValuesList.size() != numOfC) {
						message = dvcQueue.retrieveMessage();
						if (message != null) {
							dvcQueue.deleteMessage(message);
							json = message.getMessageContentAsString();

							// parse json string
							obj = JSONValue.parse(json);
							JSONArray array = (JSONArray) obj;
							List<Integer> tempCardinalityList = new ArrayList<>();

							for (int i = 0; i < array.size(); i++) {
								tempCardinalityList.add((int) array.get(i));
							}
							cardinalityValuesList.add(tempCardinalityList);
						}
					}
					generatePriorityIndex();

					// Store the priorityIndex to the azure table
					storePriorityIndex();

					// Split the images' descriptor vectors to M image sorter
					// nodes to start the comparison
					sortImagesDescriptorVectors();
					
					//Send message to the Global Image Sorter to retrieve the numberOfImageSorterNodes L^{(m)}
					initializeGlobalImageSorter();
				}
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void initializeGlobalImageSorter() {
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
					.getQueueReference(globalImageSorterQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();

			// Create the json object with the appropriate variables
			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("numOfL", new Integer(numberOfImageSorterNodes));
			obj.put("numOfImages", new Integer(numOfImages));

			// Send the Message
			CloudQueueMessage message = new CloudQueueMessage(
					obj.toJSONString());
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

	private static void sortImagesDescriptorVectors() {
		int step = (int) numOfImages / numberOfImageSorterNodes;
		for (int i = 0; i < numberOfImageSorterNodes; i++) {
			int startImageId = (i * step) + 1;
			int stopImageId = (i * step) + step;
			// Initialize the DVCExtractor Nodes
			sendMessageToImageSorterNodes(startImageId, stopImageId);
		}
	}

	private static void sendMessageToImageSorterNodes(int startImageId,
			int stopImageId) {
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
					.getQueueReference(imageSorterNodesQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();

			// Create the json object with the appropriate variables
			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("startImageId", new Integer(startImageId));
			obj.put("stopImageId", new Integer(stopImageId));

			// Send the Message
			CloudQueueMessage message = new CloudQueueMessage(
					obj.toJSONString());
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

	private static void storePriorityIndex() {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();

			String json = JSONValue.toJSONString(priorityIndex);

			PriorityIndexTableEntity entity = new PriorityIndexTableEntity(
					datasetName, json);
			TableOperation insertOperation = TableOperation.insert(entity);

			tableClient.execute(priorityIndexTable, insertOperation);

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

	private static void generatePriorityIndex() {
		int dimensions = cardinalityValuesList.get(0).size();
		priorityIndex = new ArrayList<>();
		// aggregate the M C^{(m)} cardinality values vectors to create the
		// priority indexer
		for (int i = 0; i < cardinalityValuesList.size(); i++) {
			for (int j = 0; j < dimensions; j++) {
				int value = cardinalityValuesList.get(i).get(j);
				if (value != 0) {
					PriorityIndexValue entity = new PriorityIndexValue(value, j);
					priorityIndex.add(entity);
				}
			}
		}

		// Sort the cardinality Values of each dimension in descending order
		Collections.sort(priorityIndex);
	}

	private static void initialize(boolean schedulerQueueFlag) {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the queue client
			CloudQueueClient queueClient = storageAccount
					.createCloudQueueClient();

			// Retrieve a reference to a queue
			if (schedulerQueueFlag) {
				schedulerQueue = queueClient
						.getQueueReference(priorityIndexQueue);
			} else {
				dvcQueue = queueClient.getQueueReference(dvcPriorityIndexQueue);
			}
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
