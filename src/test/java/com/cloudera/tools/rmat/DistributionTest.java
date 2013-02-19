package com.cloudera.tools.rmat;

import static org.junit.Assert.fail;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

@SuppressWarnings("unused")
public class DistributionTest {

	@Test
	public void test() {
		float [] p = {0.7f, 0.15f, 0.1f, 0.05f};
		Distribution d = new Distribution(p);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testLengthZero() {
		float [] p = {};
		Distribution d = new Distribution(p);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testElementGreaterThanOne() {
		float [] p = {0.7f, 1.5f, 0.1f};
		Distribution d = new Distribution(p);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testElementLessThanZero() {
		float [] p = {0.7f, -0.15f, 0.1f};
		Distribution d = new Distribution(p);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testElementSumGreaterThanOne() {
		float [] p = {0.7f, 0.7f, 0.1f};
		Distribution d = new Distribution(p);
	}
	
	@Test
	public void testSerde() throws IOException {
		DataOutputBuffer out = new DataOutputBuffer();
		DataInputBuffer in = new DataInputBuffer();
		
		float [] p = {0.7f, 0.15f, 0.1f, 0.05f};;
		Distribution d = new Distribution(p);
		d.write(out);
		
		Distribution e = new Distribution();
		in.reset(out.getData(), out.getLength());
		e.readFields(in);
		
		Assert.assertEquals(d, e);
	}

	@Test
	public void testStringConstructor() throws IOException {
		float [] p = {0.7f, 0.15f, 0.1f, 0.05f};;
		Distribution expected = new Distribution(p);
		
		Distribution actual = new Distribution("0.7,0.15,0.1,0.05");
		Assert.assertEquals(expected, actual);
	}
}
