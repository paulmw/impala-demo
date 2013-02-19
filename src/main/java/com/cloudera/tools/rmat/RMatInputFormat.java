package com.cloudera.tools.rmat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RMatInputFormat extends InputFormat<Edge, NullWritable> {

	public static final String NODES = "rmat.nodes";
	public static final String EDGES = "rmat.edges";
	public static final String MAPPERS = "rmat.mappers";
	public static final String RANDOM = "rmat.random";
	public static final String DISTRIBUTION = "rmat.distribution";
	
	public static void setNodes(Configuration conf, long nodes) {
		conf.setLong(NODES, nodes);
	}
	
	public static long getNodes(Configuration conf) {
		return conf.getLong(NODES, 0L);
	}
	
	public static void setEdges(Configuration conf, long edges) {
		conf.setLong(EDGES, edges);
	}
	
	public static long getEdges(Configuration conf) {
		return conf.getLong(EDGES, 0L);
	}
	
	public static void setMappers(Configuration conf, int mappers) {
		conf.setInt(MAPPERS, mappers);
	}
	
	public static int getMappers(Configuration conf) {
		return conf.getInt(MAPPERS, 0);
	}
	
	public static void setDistribution(Configuration conf, Distribution d) {
		if(!(d.length() == 4)) {
			throw new IllegalArgumentException();
		}
		d.write(conf, DISTRIBUTION);
	}
	
	public static Distribution getDistribution(Configuration conf) {
		Distribution d = new Distribution();
		d.readFields(conf, DISTRIBUTION);
		return d;
	}
	
	public static boolean getRandom(Configuration conf) {
		return conf.getBoolean(RANDOM, true);
	}
	
	public static void setRandom(Configuration conf, boolean r) {
		conf.setBoolean(RANDOM, r);
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		long nodes = getNodes(conf);
		long edges = getEdges(conf);
		int mappers = getMappers(conf);
		boolean random = getRandom(conf);
		
		long edgesPerMapper = edges / mappers;
		
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for(int i = 0; i < mappers; i++) {
			if(random) {
				splits.add(new RMatSplit(-1, nodes, edgesPerMapper));
			} else {
				splits.add(new RMatSplit(i, nodes, edgesPerMapper));
			}
		}
		return splits;
	}



	@Override
	public RecordReader<Edge, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		RMatRecordReader recordReader = new RMatRecordReader();
		recordReader.initialize(split, context);
		return recordReader;
	}



}
