package test;

import java.io.*;
import java.lang.*;

class ListElem {
	private int id;
	private float dist;

	ListElem(int dimension, float value, int id) 
	{
		this.dist = value;
		this.id = id;
	}

	ListElem(int dimension, float value) 
	{
		this.dist = value;
	}

	void setDist(float value) { this.dist = value; }

	float getDist() { return this.dist; }

	int getId() { return this.id; }

	public String toString() 
	{
		return Integer.toString(this.id) + " " + Float.toString(this.dist);	
	}

	//public static void main(String []args) 
	//{
	//}
}
