package com.cloudera.tools.rmat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.math.util.MathUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Edge implements WritableComparable<Edge> {

	private long from;
	private long to;
	private int timestamp;
	private int amount;
	
	public Edge() {}

	public Edge(long from, long to, int timestamp, int amount) {
		this.from = from;
		this.to = to;
		this.timestamp = timestamp;
		this.amount = amount;
	}

	public long getFrom() {
		return from;
	}

	public void setFrom(long from) {
		this.from = from;
	}

	public long getTo() {
		return to;
	}

	public void setTo(long to) {
		this.to = to;
	}
	
	public int getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}
	
	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(from);
		out.writeLong(to);
		out.writeInt(timestamp);
		out.writeInt(amount);
	}

	public void readFields(DataInput in) throws IOException {
		from = in.readLong();
		to = in.readLong();
		timestamp = in.readInt();
		amount = in.readInt();
	}

	public int compareTo(Edge that) {
		int comparison = (int) (this.from - that.from);
		if(comparison == 0) {
			comparison = (int) (this.to - that.to);
		}
		if(comparison == 0) {
			comparison = (int) (this.timestamp - that.timestamp);
		}
		if(comparison == 0) {
			comparison = (int) (this.amount - that.amount);
		}
		return comparison;
	}
	


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + amount;
		result = prime * result + (int) (from ^ (from >>> 32));
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		result = prime * result + (int) (to ^ (to >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Edge other = (Edge) obj;
		if (amount != other.amount)
			return false;
		if (from != other.from)
			return false;
		if (timestamp != other.timestamp)
			return false;
		if (to != other.to)
			return false;
		return true;
	}

	public String toString() {
		return "acc-" + from + ",acc-" + to + "," + timestamp + "," + amount;
	}

	public static class GroupingComparator extends WritableComparator {

		protected GroupingComparator() {
			super(Edge.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Edge ea = (Edge) a;
			Edge eb = (Edge) b;
			return (int) MathUtils.sign(ea.getFrom() - eb.from);
		}
		
	}
	
	public static class SortingComparator extends WritableComparator {

		protected SortingComparator() {
			super(Edge.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Edge ea = (Edge) a;
			Edge eb = (Edge) b;
			return ea.compareTo(eb);
		}
		
	}
}
