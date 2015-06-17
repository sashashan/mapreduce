package test;

import java.io.*;
import org.apache.hadoop.io.*;

public class BPhase1Value implements WritableComparable<BPhase1Value> {

	private Text first;
	private IntWritable second;
	private ByteWritable third;

	public BPhase1Value() {
		set(new Text(), new IntWritable(), new ByteWritable());
	}	

	public BPhase1Value(String first, int second, byte third) {
		set(new Text(first), new IntWritable(second), new ByteWritable(third));
	}

	public void set(Text first, IntWritable second, ByteWritable third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	public Text getFirst() {
		return first;
	}

	public IntWritable getSecond() {
		return second;
	}

	public ByteWritable getThird() {
		return third;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		third.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		third.readFields(in);
	}

	// Never use this method, so it doesn't matter
	@Override 
	public int hashCode() {
		return first.hashCode() * 163 + third.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BPhase1Value) {
			BPhase1Value bp1v = (BPhase1Value) o;
			return first.equals(bp1v.first) && third.equals(bp1v.third)
				&& second.equals(bp1v.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + " " + second.toString() + " " + third.toString();	
	}

	// Never used
	@Override
	public int compareTo(BPhase1Value bp2k) {
		int cmp = first.compareTo(bp2k.first);
		if (cmp != 0) {
			return cmp;	
		}
		return third.compareTo(bp2k.third);	
	}
}
