package edu.csd;

import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class PositionListEntity extends TableServiceEntity{
	private String datasetName;
	private String jsonPositionList;
	
	public PositionListEntity(String datasetName, String jsonPositionList) {
		this.partitionKey = datasetName;
		this.rowKey = datasetName;
		this.datasetName = datasetName;
		this.jsonPositionList = jsonPositionList;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public String getJsonPositionList() {
		return jsonPositionList;
	}

	public void setJsonPositionList(String jsonPositionList) {
		this.jsonPositionList = jsonPositionList;
	}
}
