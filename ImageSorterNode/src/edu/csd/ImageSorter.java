package edu.csd;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

public class ImageSorter {

	public static final String storageConnectionString = "UseDevelopmentStorage=true";
	private static CloudQueue queue;
	private final static String imageSorterNodesQueue = "imagesorterqueue";
	private final static String imageSorterToGlobalImageSorterQueue = "imagesortertoglobalqueue";
	private final static String priorityIndexTable = "priorityindex";
	private static List<DescriptorVectorEntity> descriptorList;
	private static List<PriorityIndexValue> priorityIndex;
	private static String datasetName;
	private static List<Integer> L;

	public static void main(String[] args) {
		L = new ArrayList<>();
		descriptorList = new ArrayList<>();
		priorityIndex = new ArrayList<>();
		// initialize the imagesorterqueue which retrieves the message from
		// the priorityindexer
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
					int startImageId = (Integer) jsonObject.get("startImageId");
					int stopImageId = (Integer) jsonObject.get("stopImageId");

					retrieveTheDescriptorVectors(startImageId, stopImageId);

					retrieveThePriorityIndexer();

					// Sort the descriptor vectors in descending order based on
					// the priority index;
					Collections.sort(descriptorList,
							new Comparator<DescriptorVectorEntity>() {

								@Override
								public int compare(DescriptorVectorEntity o1,
										DescriptorVectorEntity o2) {
									String vector1 = o1.getVector();
									String vector2 = o2.getVector();
									String[] splitVector1 = vector1.split(",");
									String[] splitVector2 = vector2.split(",");
									for (int i = 0; i < priorityIndex.size(); i++) {
										double value1 = Double
												.parseDouble(splitVector1[priorityIndex
														.get(i).getDimension()]);
										double value2 = Double
												.parseDouble(splitVector2[priorityIndex
														.get(i).getDimension()]);
										if (value1 > value2) {
											return 1;
										} else if (value1 < value2) {
											return -1;
										} else {
											return 0;
										}
									}
									return 0;
								}

							});

					// Generate the L^{(m)}
					for (int i = 0; i < descriptorList.size(); i++) {
						L.add(descriptorList.get(i).getId());
					}

					// Send the L^{(m)} to the global image sorter
					sendMessageToGlobalImageSorter();
				}
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private static void sendMessageToGlobalImageSorter() {
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
					.getQueueReference(imageSorterToGlobalImageSorterQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();

			// Convert the list with the cardinality values to json string
			String jsonString = JSONValue.toJSONString(L);

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

	private static void retrieveThePriorityIndexer() {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();

			TableOperation retrieveEntity = TableOperation.retrieve(
					datasetName, datasetName, PriorityIndexTableEntity.class);
			PriorityIndexTableEntity entity = tableClient.execute(
					priorityIndexTable, retrieveEntity).getResultAsType();
			// parse json string
			Object obj = JSONValue.parse(entity.getPriorityIndexJson());
			JSONArray array = (JSONArray) obj;
			for (int i = 0; i < array.size(); i++) {
				priorityIndex.add((PriorityIndexValue) array.get(i));
			}

		} catch (InvalidKeyException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void retrieveTheDescriptorVectors(int startImageId,
			int stopImageId) {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();

			for (int i = startImageId; i <= stopImageId; i++) {
				TableOperation retrieveEntity = TableOperation.retrieve(
						datasetName, Integer.toString(i),
						DescriptorVectorEntity.class);
				DescriptorVectorEntity entity = tableClient.execute(
						datasetName, retrieveEntity).getResultAsType();
				descriptorList.add(entity);
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
			queue = queueClient.getQueueReference(imageSorterNodesQueue);
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
