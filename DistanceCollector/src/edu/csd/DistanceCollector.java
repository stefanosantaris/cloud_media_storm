package edu.csd;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.queue.client.CloudQueue;
import com.microsoft.windowsazure.services.queue.client.CloudQueueClient;
import com.microsoft.windowsazure.services.queue.client.CloudQueueMessage;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableOperation;

public class DistanceCollector {

	public static final String storageConnectionString = "UseDevelopmentStorage=true";
	private final static String distanceCollectorQueue = "distancecollectorqueue";
	private static String datasetName;
	private static int range;
	private final static int topk = 100;
	private static CloudQueue queue;
	private static PriorityQueue<ResultEntity> resultHeap;
	
	public static void main(String[] args) {
		initialize();
		resultHeap = new PriorityQueue<>(2*range, new Comparator<ResultEntity>() {

			@Override
			public int compare(ResultEntity o1, ResultEntity o2) {
				if(o1.getResult() > o2.getResult()) 
					return -1;
				if(o1.getResult() < o2.getResult()) 
					return 1;
				return 0;
			}
		});
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
					double result = (double) jsonObject.get("result");
					int candidateId = (int) jsonObject.get("candidateId");
					range = (int) jsonObject.get("range");
					
					ResultEntity entity = new ResultEntity(candidateId, result);
					resultHeap.offer(entity);
					
					//If all the candidate descriptor vectors have been compared
					if(resultHeap.size() == (2 * range)) {
						storeTheResultSet();
					}
				}
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	private static void storeTheResultSet() {
		List<ResultEntity> entityList = new ArrayList<>();
		for(int i = 0; i < topk; i++) {
			entityList.add(resultHeap.poll());
		}
		
		RSetEntity rSet = new RSetEntity(datasetName, new JSONValue().toJSONString(entityList));
		
		try {
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			CloudTableClient tableClient = storageAccount.createCloudTableClient();
			TableOperation operation = TableOperation.insert(rSet);
			tableClient.execute(datasetName+"rset", operation);
			
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
			queue = queueClient
					.getQueueReference(distanceCollectorQueue);

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
