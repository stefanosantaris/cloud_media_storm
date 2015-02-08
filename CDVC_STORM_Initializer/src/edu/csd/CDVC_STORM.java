package edu.csd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.json.simple.JSONObject;

import com.microsoft.windowsazure.services.blob.client.BlobContainerPermissions;
import com.microsoft.windowsazure.services.blob.client.BlobContainerPublicAccessType;
import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.queue.client.CloudQueue;
import com.microsoft.windowsazure.services.queue.client.CloudQueueClient;
import com.microsoft.windowsazure.services.queue.client.CloudQueueMessage;

public class CDVC {
	
	public static final String storageConnectionString = "UseDevelopmentStorage=true";
	
	
	private final static String filesBlobName = "datasetcontainer";
	private final static String schedulerQueue = "schedulerqueue";
	
	public static void main(String[] args) throws IOException {
		char c = '0';
		do{
			System.out.println("\n----------------------------------------------------------------------------");
			System.out.println("\nCDVC-Menu:");
			System.out.println("\n----------------------------------------------------------------------------");
			System.out.println("\n1. Load Dataset + Preprocessing.");
			System.out.println("\n2. Insert a new Image Descriptor Vector.");
			System.out.println("\n3. CDVC k-NN Similarity Queries From File.");
			System.out.println("\n----------------------------------------------------------------------------");
			System.out.println("\n0. Exit.");
			System.out.println("\n----------------------------------------------------------------------------\n\n");
			c = (char) System.in.read();
			if(c == '1') {
				executePreprocessing();
			} else if (c == '2') {
				executeInsertion();
			} else if(c == '3') {
				executeQuery();
			}else if (c == '0') {
				System.out.println("Bye");
			} else {
				System.out.println("Wrong Number. Please insert a new number!");
			}
		} while(c != '0');
	}

	private static void executeQuery() {
		
	}

	private static void executeInsertion() {
		// TODO Auto-generated method stub
		
	}

	private static void executePreprocessing() {
		//Load the file with the images descriptor vectors to the
		//blob storage to be retrieved by the CDVC_Scheduler
		
		// Request for the dataset file.
		String datasetFile = askForFile();
		String[] splitDatasetFilePath = datasetFile.split("\\");
		String cleanDatasetFileName = splitDatasetFilePath[splitDatasetFilePath.length];
		
		uploadDatasetFile(datasetFile, cleanDatasetFileName);
		sendMessage(cleanDatasetFileName, 1);
	}

	private static void sendMessage(String fileName,
			int functionality) {
		
		try {
			//Retrieve storage account from connection-string 
			//(The storage connection string needs to be changed in case of cloud infrastructure
			//is used instead of emulator)
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			
			// Create the queue client
			CloudQueueClient queueClient = storageAccount.createCloudQueueClient();
		
			// Retrieve a reference to a queue
			CloudQueue queue = queueClient.getQueueReference(schedulerQueue);

			// Create the queue if it doesn't already exist
			queue.createIfNotExist();
			
			//Create the json object with the appropriate variables
			JSONObject obj = new JSONObject();
			obj.put("file", fileName);
			obj.put("functionality", new Integer(functionality));
			
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

	private static void uploadDatasetFile(String datasetFile,
			String cleanDatasetFileName) {
		try {
			
			//Retrieve storage account from connection-string 
			//(The storage connection string needs to be changed in case of cloud infrastructure
			//is used instead of emulator)
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			
			//Create the blob client
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
			
			// Get a reference to a container
			// The container name must be lower case
			CloudBlobContainer container = blobClient.getContainerReference(filesBlobName);
			
			// Create the container if it does not exist
			container.createIfNotExist();
			
			// Create a permissions object
			BlobContainerPermissions containerPermissions = new BlobContainerPermissions();

			// Include public access in the permissions object
			containerPermissions.setPublicAccess(BlobContainerPublicAccessType.OFF);

			// Set the permissions on the container
			container.uploadPermissions(containerPermissions);
			
			// Create or overwrite the file blob with contents from a local file
			CloudBlockBlob blob = container.getBlockBlobReference(cleanDatasetFileName);
			File source = new File(datasetFile);
			blob.upload(new FileInputStream(source), source.length());
			
			
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static String askForFile() {
		System.out.println("Please insert the file with the images' descriptor vectors to be preprocessed");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String datasetFile = null;
		try {
			datasetFile = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return datasetFile;
	}

	
	

}
