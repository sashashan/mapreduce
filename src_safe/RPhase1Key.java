package test;
import java.io.*;
import org.apache.hadoop.io.*;

public class RPhase1Key implements WritableComparable<RPhase1Key> 
{
	private Text first;
	private IntWritable second;
	private IntWritable third;

	public RPhase1Key() {
		set(new Text(), new IntWritable(), new IntWritable());
	}	

	public RPhase1Key(String first, int second, int third) {
		set(new Text(first), new IntWritable(second), new IntWritable(third));
	}

	public void set(Text first, IntWritable second, IntWritable third) {
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

	public IntWritable getThird() {
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

	@Override 
	public int hashCode() {
		return first.hashCode() * 163 + third.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof RPhase1Key) {
			RPhase1Key bp2v = (RPhase1Key) o;
			return first.equals(bp2v.first) && third.equals(bp2v.third)
				&& second.equals(bp2v.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + " " + second.toString() + " " + third.toString();	
	}

	// Never used
	@Override
	public int compareTo(RPhase1Key rp1k) {
		int cmp = first.compareTo(rp1k.first);
		if (cmp != 0) {
			return cmp;	
		}
		return third.compareTo(rp1k.third);	
	}
}
