package com.shgc.otrack.dataclean;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DataCleanMapper extends MapReduceBase implements Mapper<LongWritable, Text, StringPair, Text> {
	
	@Override
	public void close() throws IOException {
		super.close();
		System.out.println("Mapper finished");
	}

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		
		
		
		System.out.println("Mapper started");
	}

	public void map(LongWritable intputKey, Text inputText,
			OutputCollector<StringPair, Text> outputColl, Reporter rpt) throws IOException {
		
		String val = inputText.toString();
		String[] values = val.split(",");
		
		if(3 == values.length)
		{
			String date = values[0];
			date = date.replaceAll("[-:]", "");
			StringPair strPair = new StringPair();
			strPair.setMacCaptured(values[1]);
			strPair.setMacDevice(values[2]);
			//values[1] should be mac address, values[0] should be timestamp
			outputColl.collect(strPair, new Text(date));		
		}
		
	}

}
