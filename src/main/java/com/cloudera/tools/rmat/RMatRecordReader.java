package com.cloudera.tools.rmat;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RMatRecordReader extends RecordReader<Edge, NullWritable> {

	private long nodes = -1;
	private long edgesGenerated = 0;
	private long edgesPerSplit = 0;
	private CumulativeDistribution c;

	private Random random;
	
	private Edge edge = new Edge();
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		Distribution d = RMatInputFormat.getDistribution(conf);
		c = d.toCumulative();

		System.out.println("Cumulative dist is: " + c);
		
		RMatSplit rmatSplit = (RMatSplit) split;
		edgesPerSplit = rmatSplit.getEdgesPerSplit();
		nodes = rmatSplit.getNodes();
		long seed = rmatSplit.getSeed();
		if(seed == -1) {
			random = new Random();
		} else {
			random = new Random(seed);
		}
	}

	int generateQuadrant(float d) {
		if(d < c.get(0)) return 0;
		if(d < c.get(1)) return 1;
		if(d < c.get(2)) return 2;
		return 3;
	}
	
	/*
	 * The following diagram shows how the quadrants are assigned,
	 * and how the parameters relate to the layout.
	 * 
	 *      x0 --- x1
	 *  y0  +---+---+
	 *   |  | 0 | 1 |
	 *   |  +---+---+
	 *   |  | 2 | 3 |
	 *  y1  +---+---+
	 *  
	 */
	 void generateEdge() {
		long x0 = 0, y0 = 0, x1 = nodes-1, y1 = nodes-1;
		while(x0 != x1 && y0 != y1) {
			long dx = (long) Math.ceil(((float) x1 - (float) x0) / (float) 2);
			long dy = (long) Math.ceil(((float) y1 - (float) y0) / (float) 2);
			int quadrant = generateQuadrant(random.nextFloat());
			if(quadrant == 0) {
				x1 -= dx;
				y1 -= dy;
			}
			else if(quadrant == 1) {
				x0 += dx;
				y1 -= dy;
			} else if(quadrant == 2) {
				x1 -= dx;
				y0 += dy;
			} else if(quadrant == 3) {
				x0 += dx;
				y0 += dy;
			} else {
				throw new IllegalStateException("Unknown quadrant: " + quadrant);
			}
		}
		edge.setFrom(x0);
		edge.setTo(y0);
		int time = (int) (System.currentTimeMillis() / 1000);
		time += random.nextInt(86400);
		edge.setTimestamp(time);
		edge.setAmount(random.nextInt(1000));
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(edgesGenerated < edgesPerSplit) {
			generateEdge();
			while(edge.getFrom() == edge.getTo()) {
				generateEdge();
			}
			edgesGenerated++;
			return true;
		}
		return false;
	}

	@Override
	public Edge getCurrentKey() throws IOException,
			InterruptedException {
		return edge;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) edgesGenerated / (float) edgesPerSplit;
	}

	@Override
	public void close() throws IOException {
		// NOP
	}

}
