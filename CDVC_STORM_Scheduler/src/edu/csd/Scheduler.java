package edu.csd;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.microsoft.windowsazure.services.blob.client.CloudBlob;
import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.blob.client.ListBlobItem;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.queue.client.CloudQueue;
import com.microsoft.windowsazure.services.queue.client.CloudQueueClient;
import com.microsoft.windowsazure.services.queue.client.CloudQueueMessage;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableBatchOperation;

public class Scheduler {
	public static final String storageConnectionString = "UseDevelopmentStorage=true";

	private final static int numberOfDVCExtractorNodes = 2;
	private final static double W = 0.0025;

	private final static String schedulerQueue = "schedulerqueue";
	private final static String dvcExtractorQueue = "dvcextractorqueue";
	private final static String insertionQueue = "insertionqueue";
	private final static String priorityIndexerQueue = "priorityindexqueue";
	private final static String filesBlobName = "datasetcontainer";
	private static List<Integer> insertionIdsList = new ArrayList<>();

	
	private static int D, N;
	private static String datasetName;
	
	private static CloudQueue queue;

	public static void main(String[] args) {
		initialize();
		// Scheduler always is waiting for new requests-messages
		while (true) {
			try {
				CloudQueueMessage message = queue.retrieveMessage();
				if (message != null) {
					queue.deleteMessage(message);
					String json = message.getMessageContentAsString();

					// parse json string
					Object obj = JSONValue.parse(json);
					JSONObject jsonObject = (JSONObject) obj;
					String fileName = (String) jsonObject.get("file");
					int functionality = (Integer) jsonObject
							.get("functionality");

					if (functionality == 1) {
						// Initialize the preprocessing step.
						intializePreprocessing(fileName);
					} else if (functionality == 2) {
						// Initialize the insertion step.
						initializeInsertion(fileName, "n");
					} else if(functionality == 3) {
						initializeInsertion(fileName, "y");
					}
				}
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void initializeInsertion(String fileName, String query) {
		//download the file with the images descriptor vector
		downloadFile(fileName);
		
		// read the file and store the descriptor vector to the azure table
		assignVectorsToTable(fileName, false);
		
		//send message to the Dataset Updater component
		sendMessageToInsertionComponent(fileName, query);
		
	}



	private static void sendMessageToInsertionComponent(String fileName, String query) {
		String json = JSONValue.toJSONString(insertionIdsList);
		
		try {
			//Retrieve storage account from connection-string 
			//(The storage connection string needs to be changed in case of cloud infrastructure
			//is used instead of emulator)
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			
			// Create the queue client
			CloudQueueClient queueClient = storageAccount.createCloudQueueClient();
		
			// Retrieve a reference to a queue
			CloudQueue queue = queueClient.getQueueReference(insertionQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();
			
			//Create the json object with the appropriate variables
			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("idlist", json);
			obj.put("query", query);
			obj.put("numOfImages", new Integer(N));
			obj.put("W", new Double(W));
			
			//Send the Message
			CloudQueueMessage message = new CloudQueueMessage(obj.toJSONString());
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

	private static void intializePreprocessing(String fileName) {

		// download dataset file
		downloadFile(fileName);

		// read the file and store the descriptor vectors to the azure table
		assignVectorsToTable(fileName, true);
		
		// send messages and start the preprocessing step
		startPreprocessing();
		
		// send message to the priority indexer to define the number of generated c^m cardinality value vectors
		initializePriorityIndexer();
		
	}
	
	private static void initializePriorityIndexer() {
		try {
			//Retrieve storage account from connection-string 
			//(The storage connection string needs to be changed in case of cloud infrastructure
			//is used instead of emulator)
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			
			// Create the queue client
			CloudQueueClient queueClient = storageAccount.createCloudQueueClient();
		
			// Retrieve a reference to a queue
			CloudQueue queue = queueClient.getQueueReference(priorityIndexerQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();
			
			//Create the json object with the appropriate variables
			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("numOfImages", new Integer(N));
			obj.put("numOfC", new Integer(numberOfDVCExtractorNodes));
			
			//Send the Message
			CloudQueueMessage message = new CloudQueueMessage(obj.toJSONString());
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


	private static void startPreprocessing() {
		int step = (int) D / numberOfDVCExtractorNodes;
		for(int i = 0; i < numberOfDVCExtractorNodes; i++) {
			int startDimension = (i * step) + 1;
			int stopDimension = (i * step) + step;
			//Initialize the DVCExtractor Nodes
			sendMessageToDVCExtractorNodes(startDimension, stopDimension);
		}
	}
	
	private static void sendMessageToDVCExtractorNodes(int startDimension, int stopDimension) {
		try {
			//Retrieve storage account from connection-string 
			//(The storage connection string needs to be changed in case of cloud infrastructure
			//is used instead of emulator)
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			
			// Create the queue client
			CloudQueueClient queueClient = storageAccount.createCloudQueueClient();
		
			// Retrieve a reference to a queue
			CloudQueue queue = queueClient.getQueueReference(dvcExtractorQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();
			
			//Create the json object with the appropriate variables
			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("size", new Integer(N));
			obj.put("startDimension", new Integer(startDimension));
			obj.put("stopDimension", new Integer(stopDimension));
			
			//Send the Message
			CloudQueueMessage message = new CloudQueueMessage(obj.toJSONString());
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

	private static void assignVectorsToTable(String fileName, boolean preprocessingFlag) {
		//Retrieve the datasets name
		String[] splitFileName = fileName.split(".");
		datasetName = splitFileName[0];
		// In the dataset file each row constitutes an image descriptor vector.
		// The descriptors need to be comma separated.
		
		try {
			FileInputStream fstream = new FileInputStream(fileName);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine = br.readLine();
			String[] splitLine = strLine.split(",");
			// identify the dimension of the vectors
			D = splitLine.length;
			
			List<DescriptorVectorEntity> entityList = new ArrayList<>();
			
			int id;
			if(preprocessingFlag) {
				id = 1;
			} else {
				id = N + 1;
				insertionIdsList.add(id);
			}
			
			DescriptorVectorEntity entity = new DescriptorVectorEntity(
					datasetName, id, strLine);
			//The entity is inserted into the entityList in order to execute a batch insertion to the azure tables
			entityList.add(entity);
			id++;

			while ((strLine = br.readLine()) != null) {
				entity = new DescriptorVectorEntity(datasetName, id, strLine);
				entityList.add(entity);
				id++;
				
				if (entityList.size() == 100) {
					storeEntities(datasetName, entityList);
					entityList.clear();
				}
			}
			if(!entityList.isEmpty()) {
				storeEntities(datasetName, entityList);
			}
			
			N = id;
			br.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void storeEntities(String datasetName, List<DescriptorVectorEntity> entityList) {
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			// Create the table client.
			CloudTableClient tableClient = storageAccount
					.createCloudTableClient();
			
			// Define a batch operation.
			TableBatchOperation batchOperation = new TableBatchOperation();
			for (DescriptorVectorEntity entity : entityList) {
				batchOperation.insert(entity);
			}
			tableClient.execute(datasetName, batchOperation);
			
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

	private static void downloadFile(String fileName) {
		// download dataset file from the blob storager.
		// Retrieve storage account from connection-string
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);

			// Create the blob client
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

			// Retrieve reference to a previously created container
			CloudBlobContainer container = blobClient
					.getContainerReference(filesBlobName);

			// For each item in the container
			for (ListBlobItem blobItem : container.listBlobs()) {

				// If the item is a blob, not a virtual directory
				if (blobItem instanceof CloudBlob) {

					if (((CloudBlob) blobItem).getName().equals(fileName)) {
						// Download the item and save it to a file with the same
						// name
						CloudBlob blob = (CloudBlob) blobItem;
						blob.download(new FileOutputStream(blob.getName()));
					}
				}
			}

		} catch (InvalidKeyException | URISyntaxException | StorageException
				| IOException e) {
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
			queue = queueClient.getQueueReference(schedulerQueue);

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
