package com.shgc.otrack.dataclean;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;



public class DataCleanDriver {

	public static void main(String[] args){
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(com.shgc.otrack.dataclean.DataCleanDriver.class);
		
        // Mapper类型
		conf.setMapperClass(DataCleanMapper.class);
        // Reducer类型
		conf.setReducerClass(DataCleanReducer.class);
        // 分区函数.
		conf.setPartitionerClass(DevMacPartitioner.class);
        // 分组函数
		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		
        // map 输出Key的类型
		conf.setMapOutputKeyClass(StringPair.class);
        // map输出Value的类型
		conf.setMapOutputValueClass(Text.class);
        // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		conf.setOutputKeyClass(Text.class);
        // rduce输出Value的类型
		conf.setOutputValueClass(Text.class);
 
        // 输入hdfs路径
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        
        // 输出hdfs路径
        try {
			FileSystem.get(conf).delete(new Path(args[1]), true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
        // 提交job
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
