package edu.csd;

public class TempDescriptorVectorEntity extends DescriptorVectorEntity {
	private int imageSorterNode;

	public TempDescriptorVectorEntity(String filename, int id, String vector,
			int imageSorterNode) {
		super(filename, id, vector);
		this.imageSorterNode = imageSorterNode;
	}

	public int getImageSorterNode() {
		return imageSorterNode;
	}

	public void setImageSorterNode(int imageSorterNode) {
		this.imageSorterNode = imageSorterNode;
	}
}
