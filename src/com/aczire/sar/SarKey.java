package com.aczire.sar;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SarKey implements WritableComparable<Object> {
	public boolean Locked;
	public boolean Compressed;
	public String Key;
	public String Salt;
	public String Filename;
	public double FileSize;
	public String Id;

	public SarKey(boolean locked, boolean compressed) {
		this.Locked = locked;
		this.Compressed = compressed;
		this.Key = "";
		this.Salt = "";
		this.Filename = "";
		this.FileSize = 0;
		this.Id = "";
	}

	public SarKey() {
		this(false, false);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(Locked);
		out.writeBoolean(Compressed);
		out.writeUTF(Key);
		out.writeUTF(Salt);
		out.writeUTF(Filename);
		out.writeDouble(FileSize);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Locked = in.readBoolean();
		Compressed = in.readBoolean();
		Key = in.readUTF();
		Salt = in.readUTF();
		Filename = in.readUTF();
		FileSize = in.readDouble();
	}

	@Override
	public String toString() {
		return "Filename: " + Filename + ", Locked: "
				+ Boolean.toString(Locked) + ", Compressed: "
				+ Boolean.toString(Compressed);
	}

	@Override
	public boolean equals(Object o) {
		SarKey other = (SarKey)o;
		if (!(other instanceof SarKey)) {
			return false;
		}

		return this.Filename.equals(other.Filename);
	}

	@Override
	public int hashCode() {
		return Filename.hashCode()
				^ Boolean.toString(Locked).hashCode()
				^ Boolean.toString(Compressed).hashCode();
	}

	public int compareTo(SarKey other) {
		return this.Filename.compareTo(other.Filename);
	}

	@Override
	public int compareTo(Object o) {
		SarKey other = (SarKey)o;
		return this.Filename.compareTo(other.Filename);
	}
}