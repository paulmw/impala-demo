package com.cloudera.tools.rmat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RMat implements Tool {

	private Configuration conf;
	
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return conf;
	}

	public int run(String[] args) throws Exception {
		if(RMatInputFormat.getNodes(conf) == 0) {
			RMatInputFormat.setNodes(conf, 100000);
		}
		if(RMatInputFormat.getEdges(conf) == 0) {
			RMatInputFormat.setEdges(conf, 1000000);
		}
		if(RMatInputFormat.getMappers(conf) == 0) {
			RMatInputFormat.setMappers(conf, 3);
		}
		Distribution d;
		String distributionParameter = conf.get(RMatInputFormat.DISTRIBUTION, "");
		if(distributionParameter.equals("")) {
			float [] p = {0.7f, 0.15f, 0.1f, 0.05f};
			d = new Distribution(p);
		} else {
			d = new Distribution(distributionParameter);
		}
		RMatInputFormat.setDistribution(conf, d);
		RMatInputFormat.setRandom(conf, true);
		
		Job job = new Job(conf, "RMat Graph Generator");
		
		job.setOutputKeyClass(Edge.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(RMatInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		job.setJarByClass(RMat.class);
		
		job.setNumReduceTasks(0);
		
		job.submit();
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		RMat rmat = new RMat();
		ToolRunner.run(rmat, args);
	}

}
