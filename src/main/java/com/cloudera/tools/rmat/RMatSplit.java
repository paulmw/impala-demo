package com.cloudera.tools.rmat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class RMatSplit extends InputSplit implements Writable {

	private long seed;
	private long edgesPerSplit;
	private long nodes;
	
	public RMatSplit() {}
	
	public RMatSplit(long seed, long nodes, long edgesPerSplit) {
		this.seed = seed;
		this.nodes = nodes;
		this.edgesPerSplit = edgesPerSplit;
	}

	public long getSeed() {
		return seed;
	}

	public void setSeed(long seed) {
		this.seed = seed;
	}

	public long getEdgesPerSplit() {
		return edgesPerSplit;
	}

	public void setEdgesPerSplit(long edgesPerSplit) {
		this.edgesPerSplit = edgesPerSplit;
	}

	public long getNodes() {
		return nodes;
	}

	public void setNodes(long nodes) {
		this.nodes = nodes;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(seed);
		out.writeLong(nodes);
		out.writeLong(edgesPerSplit);
	}

	public void readFields(DataInput in) throws IOException {
		seed = in.readLong();
		nodes = in.readLong();
		edgesPerSplit = in.readLong();
	}

}
