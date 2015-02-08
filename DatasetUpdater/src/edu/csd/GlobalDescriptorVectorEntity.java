package edu.csd;

import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class GlobalDescriptorVectorEntity extends TableServiceEntity {

	private String descriptorVectorPartitionKey;
	private String descriptorVectorRowKey;
	private String fileName;
	private int id;
	private String vector;

	public GlobalDescriptorVectorEntity(String descriptorVectorPartitionKey,
			String descriptorVectorRowKey, String fileName, int id,
			String vector) {
		this.partitionKey = descriptorVectorPartitionKey;
		this.rowKey = descriptorVectorRowKey;
		this.descriptorVectorPartitionKey = descriptorVectorPartitionKey;
		this.descriptorVectorRowKey = descriptorVectorRowKey;
		this.fileName = fileName;
		this.id = id;
		this.vector = vector;
	}

	public String getDescriptorVectorPartitionKey() {
		return descriptorVectorPartitionKey;
	}

	public void setDescriptorVectorPartitionKey(
			String descriptorVectorPartitionKey) {
		this.descriptorVectorPartitionKey = descriptorVectorPartitionKey;
	}

	public String getDescriptorVectorRowKey() {
		return descriptorVectorRowKey;
	}

	public void setDescriptorVectorRowKey(String descriptorVectorRowKey) {
		this.descriptorVectorRowKey = descriptorVectorRowKey;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getVector() {
		return vector;
	}

	public void setVector(String vector) {
		this.vector = vector;
	}
}
