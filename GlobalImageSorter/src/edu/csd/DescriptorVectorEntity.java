package edu.csd;

import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class DescriptorVectorEntity extends TableServiceEntity{
	private String fileName;
	private int id;
	private String vector;
	
	public DescriptorVectorEntity(String filename, int id, String vector) {
		this.partitionKey = filename;
		this.rowKey = Integer.toString(id);
		this.fileName = filename;
		this.id = id;
		this.vector = vector;
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
