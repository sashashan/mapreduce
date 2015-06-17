package test;

import java.io.*;
import org.apache.hadoop.io.*;

public class RPhase1Value implements WritableComparable<RPhase1Value> 
{
	private Text first;
	private IntWritable second;
	private ByteWritable third;

	public RPhase1Value() {
		set(new Text(), new IntWritable(), new ByteWritable());
	}	

	public RPhase1Value(String first, int second, byte third) {
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

	@Override  //place holder
	public int hashCode() {
		return first.hashCode() * 163 + third.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof RPhase1Value) {
			RPhase1Value rp1v = (RPhase1Value) o;
			return first.equals(rp1v.first) && third.equals(rp1v.third)
				&& second.equals(rp1v.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + " " + second.toString() + " " + third.toString();	
	}

	@Override //place holder
	public int compareTo(RPhase1Value rp1v) 
	{
		int cmp = first.compareTo(rp1v.first);
		if (cmp != 0) {
			return cmp;	
		}
		return third.compareTo(rp1v.third);	
	}
}
