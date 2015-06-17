package test;

import java.io.*;
import org.apache.hadoop.io.*;

public class BPhase2Key implements WritableComparable<BPhase2Key> {

	private Text first;
	private ByteWritable second;

	public BPhase2Key() {
		set(new Text(), new ByteWritable());
	}	

	public BPhase2Key(String first, byte second) {
		set(new Text(first), new ByteWritable(second));
	}

	public void set(Text first, ByteWritable second) {
		this.first = first;
		this.second = second;
		//this.second.set(second);
	}

	public Text getFirst() {
		return first;
	}

	public ByteWritable getSecond() {
		return second;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BPhase2Key) {
			BPhase2Key bp2k = (BPhase2Key) o;
			return first.equals(bp2k.first) && second.equals(bp2k.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + " " + second.toString();	
	}

	@Override
	public int compareTo(BPhase2Key bp2k) {
		int cmp = first.compareTo(bp2k.first);
		if (cmp != 0) {
			return cmp;	
		}
		return second.compareTo(bp2k.second);	
	}
}
