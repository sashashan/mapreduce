package test;

import java.io.*;
import org.apache.hadoop.io.*;

public class TextBytePair implements WritableComparable<TextBytePair> {

	private Text first;
	private ByteWritable second;

	public TextBytePair() {
		set(new Text(), new ByteWritable());
	}	

	public TextBytePair(String first, byte second) {
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
		if (o instanceof TextBytePair) {
			TextBytePair tp = (TextBytePair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + " " + second.toString();	
	}

	@Override
	public int compareTo(TextBytePair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;	
		}
		return second.compareTo(tp.second);	
	}
}
