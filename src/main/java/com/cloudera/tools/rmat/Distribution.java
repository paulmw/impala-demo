package com.cloudera.tools.rmat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class Distribution implements Writable {

	private float [] p = new float[0];
	
	public Distribution() {
		// Required for Writable
	}
	
	public Distribution(String s) {
		String [] parts = s.split(",");
		float [] p = new float[parts.length];
		for(int i = 0; i < p.length; i++) {
			p[i] = Float.parseFloat(parts[i]);
		}
		checkValidity(p);
		this.p = p;
	}
	
	public Distribution(float [] p) {
		checkValidity(p);
		this.p = p;
 	}
	
	public float get(int i) {
		return p[i];
	}
	
	public int length() {
		return p.length;
	}
	
	private static void checkValidity(float [] p) {
		if(p.length == 0) {
			throw new IllegalArgumentException("float [] p should not be zero-length");
		}
		float sum = 0.0f;
		for(int i = 0; i < p.length; i++) {
			sum += p[i];
			if(p[i] < 0.0f) {
				throw new IllegalArgumentException("elements of float [] p should not be less than zero");
			}
			if(p[i] > 1.0f) {
				throw new IllegalArgumentException("elements of float [] p should not be greater than one");
			}
		}
		if(sum != 1.0f) {
			throw new IllegalArgumentException("elements of float [] p should sum to one");
		}
	}

	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(p);
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
		Distribution other = (Distribution) obj;
		if (!Arrays.equals(p, other.p))
			return false;
		return true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(p.length);
		for(int i = 0; i < p.length; i++) {
			out.writeFloat(p[i]);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		float [] p = new float[length];
		for(int i = 0; i < length; i++) {
			p[i] = in.readFloat();
		}
		checkValidity(p);
		this.p = p;
	}
	
	public void write(Configuration conf, String prefix) {
		conf.setInt(prefix + ".length", length());
		for(int i = 0; i < length(); i++) {
			conf.setFloat(prefix + "." + i, p[i]);
		}
	}
	
	public void readFields(Configuration conf, String prefix) {
		int length = conf.getInt(prefix + ".length", -1);
		if(length == -1) {
			throw new IllegalStateException("Not defined.");
		}
		float [] p = new float[length];
		for(int i = 0; i < p.length; i++) {
			p[i] = conf.getFloat(prefix + "." + i, -1);
		}
		checkValidity(p);
		this.p = p;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < p.length; i++) {
			if(i != 0) {
				sb.append(",");
			}
			sb.append(p[i]);
		}
		return sb.toString();
	}
	
	public CumulativeDistribution toCumulative() {
		float [] c = new float[p.length];
		for(int i = 0; i < p.length; i++) {
			if(i == 0) {
				c[i] = p[i];
			} else {
				c[i] = c[i-1] + p[i];
			}
		}
		return new CumulativeDistribution(c);
	}
}
