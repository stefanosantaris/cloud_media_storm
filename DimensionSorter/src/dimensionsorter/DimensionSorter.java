/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dimensionsorter;

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

/**
 *
 * @author santaris
 */
public class DimensionSorter {

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

    /**
     * @param args the command line arguments
     */
    public static void sortDimensions() {

        CloudQueueMessage message = schedulerQueue.retrieveMessage();
        if (message != null) {
            schedulerQueue.deleteMessage(message);
            String json = message.getMessageContentAsString();
            Object obj = JSONValue.parse(json);
            JSONObject jsonObject = (JSONObject) obj;
            datasetName = (String) jsonObject.get("dataset");
            double[] image_descritor_vector = (double[]) jsonObject.get("im_d_v");
            double[] priority_index = (double[]) jsonObject.get("p_i");

            double[] sorted_i_d_v = new double[image_descritor_vector.length];
            for (int i = 0; i < priority_index.length; i++) {
                sorted_i_d_v[i] = (double) image_descritor_vector[priority_index[i]];
            }

            //Send message to the Image Sorter to retrieve the numberOfImageSorterNodes L^{(m)}
            initializeImageSorter();
        }
    }

    private static void initializeImageSorter() {
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

}
