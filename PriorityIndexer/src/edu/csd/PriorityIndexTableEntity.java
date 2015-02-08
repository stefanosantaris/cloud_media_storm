package edu.csd;

import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class PriorityIndexTableEntity extends TableServiceEntity{
	private String datasetName;
	private String priorityIndexJson;
	
	public PriorityIndexTableEntity(String datasetName, String priorityIndexJson) {
		this.partitionKey = datasetName;
		this.partitionKey = datasetName;
		this.datasetName = datasetName;
		this.priorityIndexJson = priorityIndexJson;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public String getPriorityIndexJson() {
		return priorityIndexJson;
	}

	public void setPriorityIndexJson(String priorityIndexJson) {
		this.priorityIndexJson = priorityIndexJson;
	}
	
	
}
