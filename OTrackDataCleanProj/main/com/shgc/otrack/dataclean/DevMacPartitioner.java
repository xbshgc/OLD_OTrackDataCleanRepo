package com.shgc.otrack.dataclean;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * 分区函数类。根据DeviceMac确定Partition。
 */
public class DevMacPartitioner implements Partitioner<StringPair, Text>
{
	public void configure(JobConf arg0) {
		System.out.println("DevMacPartitioner created");
		
	}

	public int getPartition(StringPair key, Text val, int numPartitions) {
		System.out.println("DevMacPartitioner called");
		StringPair strPa = (StringPair)key;
		int nRet = Math.abs(strPa.getMacDevice().hashCode()) % numPartitions;
		//int nRet = Math.abs(strPa.getMacDevice().hashCode()*128 + strPa.getMacCaptured().hashCode()) % numPartitions;
		return nRet;
	}
}
