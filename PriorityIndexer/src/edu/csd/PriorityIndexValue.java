package edu.csd;

public class PriorityIndexValue implements Comparable<PriorityIndexValue>{
	public PriorityIndexValue entity;
	private int cardinalityValue;
	private int dimension;
	
	public PriorityIndexValue(int cardinalityValue, int dimension) {
		this.cardinalityValue = cardinalityValue;
		this.dimension = dimension;
		entity = new PriorityIndexValue(cardinalityValue, dimension);
	}
	
	public int getCardinalityValue() {
		return cardinalityValue;
	}
	
	public int getDimension() {
		return dimension;
	}
	
	@Override
	public int compareTo(PriorityIndexValue e1) {
		if(this.getCardinalityValue() > e1.getCardinalityValue()) 
			return -1;
		if(this.getCardinalityValue() == e1.getCardinalityValue())
			return 0;
		else return 1;
	}

}
