package edu.csd;

public class ResultEntity {
	private int id;
	private double result;
	
	public ResultEntity(int id, double result) {
		super();
		this.id = id;
		this.result = result;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public double getResult() {
		return result;
	}
	public void setResult(double result) {
		this.result = result;
	}
	
}
