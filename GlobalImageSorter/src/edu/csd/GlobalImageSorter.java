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

public class GlobalImageSorter {

	private static CloudQueue priorityQueue, imageSorterQueue;
	public static final String storageConnectionString = "UseDevelopmentStorage=true";
	private final static String globalImageSorterQueue = "globalimagesorterqueue";
	private final static String imageSorterToGlobalImageSorterQueue = "imagesortertoglobalqueue";
	private static List<List<Integer>> Llists;
	private static List<Integer> globalL;
	private static List<PriorityIndexValue> priorityIndex;
	private final static String priorityIndexTable = "priorityindex";
	private static String datasetName;
	private static int numOfImages;

	public static void main(String[] args) {
		// globalL contains the ids of the descriptor vectors into their
		// reordered position
		globalL = new ArrayList<>();

		priorityIndex = new ArrayList<>();

		// initialize the globalImageSorterQueue which retrieves the message
		// from
		// the priorityIndexer
		initialize(true);

		while (true) {
			try {
				CloudQueueMessage message = priorityQueue.retrieveMessage();
				if (message != null) {
					priorityQueue.deleteMessage(message);
					String json = message.getMessageContentAsString();

					// parse json string
					Object obj = JSONValue.parse(json);
					JSONObject jsonObject = (JSONObject) obj;
					datasetName = (String) jsonObject.get("dataset");
					int numOfL = (Integer) jsonObject.get("numOfL");
					numOfImages = (Integer) jsonObject.get("numOfImages");

					Llists = new ArrayList<>();
					// initialize the imageSorterToGlobalImageSorterQueue which
					// retrieves the
					// L^{(m)}
					initialize(false);

					while (Llists.size() != numOfL) {
						message = imageSorterQueue.retrieveMessage();
						if (message != null) {
							imageSorterQueue.deleteMessage(message);
							json = message.getMessageContentAsString();

							// parse json string
							obj = JSONValue.parse(json);
							JSONArray array = (JSONArray) obj;
							List<Integer> tempLList = new ArrayList<>();

							for (int i = 0; i < array.size(); i++) {
								tempLList.add((int) array.get(i));
							}
							Llists.add(tempLList);
						}
					}

					retrieveThePriorityIndexer();
					
					sortImages();

					storeTheUpdatedPositionList();
				}
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void storeTheUpdatedPositionList() {

		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();
			
			PositionListEntity entity = new PositionListEntity(datasetName, JSONValue.toJSONString(globalL));

			// Create an operation to add the entity to the datasetNamepositions table.
			TableOperation insertOperation = TableOperation.insert(entity);

			// Submit the operation to the table service.
			tableClient.execute(datasetName + "positions", insertOperation);

		} catch (InvalidKeyException | URISyntaxException | StorageException e) {
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

	private static void sortImages() {
		while (globalL.size() < numOfImages) {
			List<TempDescriptorVectorEntity> tempImagesList = new ArrayList<>();
			for (int i = 0; i < Llists.size(); i++) {
				if (!Llists.get(i).isEmpty()) {
					DescriptorVectorEntity entity = retrieveDescriptorVector(Llists
							.get(i).get(0));
					TempDescriptorVectorEntity tempEntity = new TempDescriptorVectorEntity(
							entity.getFileName(), entity.getId(),
							entity.getVector(), i);
					tempImagesList.add(tempEntity);

					// Create a new GlobalDescriptorVectorEntity where the
					// partition key is the dimension with the highest priority
					String vector = entity.getVector();
					String[] splitVector = vector.split(",");
					String partitionKey = splitVector[priorityIndex.get(0)
							.getDimension()];
					String rowKey = splitVector[priorityIndex.get(1)
							.getDimension()];
					GlobalDescriptorVectorEntity globalEntity = new GlobalDescriptorVectorEntity(
							partitionKey, rowKey, entity.getFileName(),
							entity.getId(), entity.getVector());
					storeTheGlobalEntity(globalEntity);
				}
			}

			// Sort the descriptor vectors in descending order based on
			// the priority index;
			Collections.sort(tempImagesList,
					new Comparator<TempDescriptorVectorEntity>() {

						@Override
						public int compare(TempDescriptorVectorEntity o1,
								TempDescriptorVectorEntity o2) {
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

			// Get the descriptor vector with the highest value;
			globalL.add(tempImagesList.get(0).getId());

			// remove the id from the corresponding L^{(m)} list
			Llists.get(tempImagesList.get(0).getImageSorterNode()).remove(0);

		}
	}

	private static void storeTheGlobalEntity(GlobalDescriptorVectorEntity entity) {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();

			// Create an operation to add the new customer to the people table.
			TableOperation insertOperation = TableOperation.insert(entity);

			// Submit the operation to the table service.
			tableClient.execute(datasetName + "ordered", insertOperation);

		} catch (InvalidKeyException | URISyntaxException | StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static DescriptorVectorEntity retrieveDescriptorVector(
			int descriptorVectorId) {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		DescriptorVectorEntity entity = null;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();

			TableOperation retrieveEntity = TableOperation.retrieve(
					datasetName, Integer.toString(descriptorVectorId),
					DescriptorVectorEntity.class);
			entity = tableClient.execute(datasetName, retrieveEntity)
					.getResultAsType();

		} catch (InvalidKeyException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return entity;
	}

	private static void initialize(boolean priorityIndexerFlag) {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the queue client
			CloudQueueClient queueClient = storageAccount
					.createCloudQueueClient();

			// Retrieve a reference to a queue
			if (priorityIndexerFlag) {
				priorityQueue = queueClient
						.getQueueReference(globalImageSorterQueue);
			} else {
				imageSorterQueue = queueClient
						.getQueueReference(imageSorterToGlobalImageSorterQueue);
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
