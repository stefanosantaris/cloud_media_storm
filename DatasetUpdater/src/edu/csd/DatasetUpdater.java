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
import com.microsoft.windowsazure.services.table.client.TableConstants;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableQuery;
import com.microsoft.windowsazure.services.table.client.TableQuery.Operators;
import com.microsoft.windowsazure.services.table.client.TableQuery.QueryComparisons;

public class DatasetUpdater {
	
	
	public static final String storageConnectionString = "UseDevelopmentStorage=true";
	private final static String insertionQueue = "insertionqueue";
	private final static String candidateImageIDRetrievalQueue = "candidateidretrievalqueue";
	private static List<Integer> insertionIdsList = new ArrayList<>();
	private static List<Integer> positionsList = new ArrayList<>();
	private static List<Integer> updatedPositionsList = new ArrayList<>();
	private static String datasetName;
	private static List<PriorityIndexValue> priorityIndex;
	private static String query;
	private static Double W;
	private static int N;

	private final static String priorityIndexTable = "priorityindex";
	private static CloudQueue queue;

	public static void main(String[] args) {
		initialize();
		priorityIndex = new ArrayList<>();

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
					String jsonIdList = (String) jsonObject.get("idlist");
					query = (String) jsonObject.get("query");
					N = (Integer) jsonObject.get("numOfImages");
					W = (Double) jsonObject.get("W");
					
					obj = JSONValue.parse(jsonIdList);
					JSONArray array = (JSONArray) obj;
					for (int i = 0; i < array.size(); i++) {
						insertionIdsList.add((int) array.get(i));
					}

					retrieveThePriorityIndexer();

					retrieveTheDescriptorVectorsPositions();

					insertImageDescriptorVector();
					

				}
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void sendMessageToQueryProcessingStep(int id, int position) {
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
					.getQueueReference(candidateImageIDRetrievalQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();

			
			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("numOfImages", new Integer(N));
			obj.put("descriptorId", new Integer(id));
			obj.put("W", new Double(W));
			obj.put("position", new Integer(position));
			
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

	private static void insertImageDescriptorVector() {
		for (int i = 0; i < insertionIdsList.size(); i++) {
			DescriptorVectorEntity entity = retrieveDescriptorVector(insertionIdsList
					.get(i));
			List<GlobalDescriptorVectorEntity> candidateEntities = retrieveCandidateEntities(entity);

			int imageId = -1;
			int max = -1;
			// Identify which image has the highest position
			for (int j = 0; j < candidateEntities.size(); j++) {
				for (int k = 0; k < positionsList.size(); k++) {
					if (candidateEntities.get(j).getId() == positionsList
							.get(k)) {
						if (k > max) {
							max = k;
							imageId = j;
						}
					}
				}
			}

			// Update the position list according to the comparison result
			int result = compareImages(candidateEntities.get(imageId), entity);
			int position;
			if (result > 0) {
				for (int j = 0; j < (max - 1); j++) {
					updatedPositionsList.add(positionsList.get(j));
				}
				updatedPositionsList.add(entity.getId());
				position = updatedPositionsList.size() - 1;
				updatedPositionsList.add(positionsList.get(max));
				for (int j = (max + 1); j < positionsList.size(); j++) {
					updatedPositionsList.add(positionsList.get(j));
				}
			} else {
				for (int j = 0; j < max; j++) {
					updatedPositionsList.add(positionsList.get(j));
				}
				updatedPositionsList.add(entity.getId());
				position = updatedPositionsList.size() - 1;
				for (int j = (max + 1); j < positionsList.size(); j++) {
					updatedPositionsList.add(positionsList.get(j));
				}
			}

			storeTheUpdatedPositionList();

			indexTheInsertedImage(entity);
			
			if(query.equals("y")) {
				sendMessageToQueryProcessingStep(entity.getId(), position);
			}
		}
	}

	private static void indexTheInsertedImage(DescriptorVectorEntity entity) {
		String insertedVector = entity.getVector();
		String[] insertedSplitVector = insertedVector.split(",");
		GlobalDescriptorVectorEntity globalEntity = new GlobalDescriptorVectorEntity(
				insertedSplitVector[priorityIndex.get(0).getDimension()],
				insertedSplitVector[priorityIndex.get(1).getDimension()],
				entity.getFileName(), entity.getId(), entity.getVector());
		storeTheGlobalEntity(globalEntity);
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

	private static void storeTheUpdatedPositionList() {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();

			PositionListEntity entity = new PositionListEntity(datasetName,
					new JSONValue().toJSONString(updatedPositionsList));

			TableOperation insertOperation = TableOperation
					.insertOrReplace(entity);

			tableClient.execute(datasetName + "positions", insertOperation);

		} catch (InvalidKeyException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static int compareImages(
			GlobalDescriptorVectorEntity globalDescriptorVectorEntity,
			DescriptorVectorEntity entity) {
		String candidateVector = globalDescriptorVectorEntity.getVector();
		String[] candidateSplitVector = candidateVector.split(",");

		String insertedVector = entity.getVector();
		String[] insertedSplitVector = insertedVector.split(",");

		for (int i = 0; i < insertedSplitVector.length; i++) {
			double insertedValue = Double
					.parseDouble(insertedSplitVector[priorityIndex.get(i)
							.getDimension()]);
			double candidateValue = Double
					.parseDouble(candidateSplitVector[priorityIndex.get(i)
							.getDimension()]);
			if (insertedValue > candidateValue) {
				return 1;
			} else if (insertedValue < candidateValue) {
				return -1;
			}
		}
		return 0;
	}

	private static List<GlobalDescriptorVectorEntity> retrieveCandidateEntities(
			DescriptorVectorEntity entity) {
		String vector = entity.getVector();
		String[] splitVector = vector.split(",");
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		List<GlobalDescriptorVectorEntity> entitiesList = new ArrayList<>();
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();

			// Create a filter condition where the partition key is "Smith".
			String partitionFilter = TableQuery.generateFilterCondition(
					TableConstants.PARTITION_KEY, QueryComparisons.EQUAL,
					splitVector[priorityIndex.get(0).getDimension()]);

			// Create a filter condition where the row key is less than the
			// letter "E".
			String rowFilter = TableQuery.generateFilterCondition(
					TableConstants.ROW_KEY, QueryComparisons.LESS_THAN,
					splitVector[priorityIndex.get(1).getDimension()]);

			// Combine the two conditions into a filter expression.
			String combinedFilter = TableQuery.combineFilters(partitionFilter,
					Operators.AND, rowFilter);

			TableQuery<GlobalDescriptorVectorEntity> rangeQuery = TableQuery
					.from(datasetName + "ordered",
							GlobalDescriptorVectorEntity.class).where(
							combinedFilter);

			for (GlobalDescriptorVectorEntity tempEntity : tableClient
					.execute(rangeQuery)) {
				entitiesList.add(tempEntity);
			}

		} catch (InvalidKeyException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return entitiesList;
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

	private static void initialize() {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the queue client
			CloudQueueClient queueClient = storageAccount
					.createCloudQueueClient();

			// Retrieve a reference to a queue
			queue = queueClient.getQueueReference(insertionQueue);

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
