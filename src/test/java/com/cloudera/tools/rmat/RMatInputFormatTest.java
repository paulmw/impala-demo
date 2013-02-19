package com.cloudera.tools.rmat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Test;

public class RMatInputFormatTest {

	@Test
	public void testGetSplits() throws IOException, InterruptedException {
		testGetSplits(100, 200, 10);
		testGetSplits(100, 200, 9);
	}

	private void testGetSplits(long nodes, long edges, int mappers) throws IOException, InterruptedException {
		Configuration conf = new Configuration(false);
		
		RMatInputFormat.setNodes(conf, nodes);
		RMatInputFormat.setEdges(conf, edges);
		RMatInputFormat.setMappers(conf, mappers);
		long edgesPerSplit = edges / mappers;
		
		JobContext context = mock(JobContext.class);
		when(context.getConfiguration()).thenReturn(conf);
		
		RMatInputFormat inputFormat = new RMatInputFormat();
		List<InputSplit> splits = inputFormat.getSplits(context);
		assertEquals(mappers, splits.size());
		for(InputSplit split : splits) {
			assertTrue(split instanceof RMatSplit);
			RMatSplit rmatSplit = (RMatSplit) split;
			assertEquals(nodes, rmatSplit.getNodes());
			assertEquals(edgesPerSplit, rmatSplit.getEdgesPerSplit());
		}
		
	}

}
