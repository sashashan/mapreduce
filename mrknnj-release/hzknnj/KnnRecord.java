package test;
import java.io.Serializable;
class KnnRecord implements Serializable {
	private int rid;
	private float dist;

	KnnRecord(int rid, float dist) {
		this.rid = rid;
		this.dist = dist;	
	}

	public float getDist() {
		return this.dist;
	}

	public int getRid() {
		return this.rid;
	}

	public String toString() {
		return this.rid + " " + Float.toString(this.dist);
	}
}
