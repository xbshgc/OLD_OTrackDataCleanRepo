package com.shgc.otrack.dataclean;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DataCleanReducer extends MapReduceBase implements Reducer<StringPair, Text, Text, Text> {
	
	final int STAY_THRESHOLD = 900 * 1000; // 900 seconds in milliseconds
	private SimpleDateFormat dateF;
	private SimpleDateFormat dateF1;
	
	
	
	@Override
	public void close() throws IOException {
		super.close();
		System.out.println("Reducer finished");
	}



	@Override
	public void configure(JobConf job) {
		super.configure(job);
		System.out.println("Reducer started");
		dateF = new SimpleDateFormat("yyyyMMddHHmmss");
		dateF1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	

	public void reduce(StringPair key, Iterator<Text> values,
			OutputCollector<Text, Text> outputColl, Reporter rpt) throws IOException {		
		
		ArrayList<Long> valSortedList = new ArrayList<Long>();
		long milliSecVal = 0;
		String outputStr;
		
		while (values.hasNext()) {
			String strDate = values.next().toString();
			try {
				milliSecVal = dateF.parse(strDate).getTime();						
			} catch (ParseException e) {
				e.printStackTrace();
			}
			valSortedList.add(milliSecVal);
		}
		Collections.sort(valSortedList);

		long min_milliSecVal = 0;
		long max_milliSecVal = 0;
		
		
		int nSize = valSortedList.size();
		if (nSize > 1) {
			min_milliSecVal = valSortedList.get(0);
			max_milliSecVal = valSortedList.get(nSize - 1);

			Date d1 = new Date(min_milliSecVal);
			String firstDate =dateF1.format(d1);
			Date d2 = new Date(max_milliSecVal);
			String lastDate =dateF1.format(d2);

			long nVisitTimes = 1;
			long duration = 0;
			long currVal = 0;
			long lastVal = valSortedList.get(0);
			long diff = 0;
			for (int i = 0; i < nSize; i++) {
				currVal = valSortedList.get(i);
				diff = currVal - lastVal;
				if (diff > STAY_THRESHOLD) {
					nVisitTimes++;
				} else {
					duration += diff;
				}
				lastVal = currVal;
			}
			
			if (0 == duration)
				duration = 1000; // set minimize value to 1 second
			duration = duration / 1000; //in seconds

			outputStr = String.format("%s\t%s\t%s\t%s\t%s", duration, nVisitTimes, firstDate, lastDate, key.getMacDevice());
			outputColl.collect(new Text(key.getMacCaptured()), new Text(outputStr));
		}
	}

}
