package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/* This class is used to store a "word" of type String and a count of type Integer */

public class WordCountWritable implements Comparable<WordCountWritable>, Writable {

	private String word; // Contains a "word"
	private Integer count; // number of occurrences of "word"

	public WordCountWritable(String word, Integer count) {
		this.word = word;
		this.count = count;
	}

	public WordCountWritable(WordCountWritable other) {
		this.word = new String(other.getWord());
		this.count = new Integer(other.getCount());
	}

	public WordCountWritable() {
	}

	public String getWord() {
		return word;
	}

	public void setWord(String pair) {
		this.word = pair;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	@Override
	public int compareTo(WordCountWritable other) {

		if (this.count.compareTo(other.getCount()) != 0) {
			return this.count.compareTo(other.getCount());
		} else { // if the count values of the two words are equal, the
					// lexicographical order is considered
			return this.word.compareTo(other.getWord());
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word = in.readUTF();
		count = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeInt(count);

	}

	public String toString() {
		return new String(word + "," + count);
	}

}
