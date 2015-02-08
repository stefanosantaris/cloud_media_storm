package edu.csd;

import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class RSetEntity extends TableServiceEntity{
	private String datasetName;
	private String R;
	
	public RSetEntity(String datasetName, String R) {
		this.partitionKey = datasetName;
		this.rowKey = datasetName;
		this.datasetName = datasetName;
		this.R = R;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public String getR() {
		return R;
	}

	public void setR(String r) {
		R = r;
	}
}
