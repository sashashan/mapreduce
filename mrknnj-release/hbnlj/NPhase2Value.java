
package test;
import java.io.*;

import org.apache.hadoop.io.*;


public class NPhase2Value implements WritableComparable<NPhase2Value> {

	private IntWritable first;
	private FloatWritable second;

	public NPhase2Value() {
		set(new IntWritable(), new FloatWritable());
	}

	public NPhase2Value(int first, float second) {
		set(new IntWritable(first), new FloatWritable(second));
	}

	public void set(IntWritable first, FloatWritable second) {
		this.first = first;
		this.second = second;	
	}

	public IntWritable getFirst() {
		return first;
	}

	public FloatWritable getSecond() {
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
	public boolean equals(Object o) {
		if (o instanceof NPhase2Value) {
			NPhase2Value np2v = (NPhase2Value) o;
			return first.equals(np2v.first) && second.equals(np2v.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first.toString() + " " + second.toString();
	}

	@Override
	public int compareTo(NPhase2Value np2v) {
		return 1;
	}

}
