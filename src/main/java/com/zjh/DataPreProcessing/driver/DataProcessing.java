package com.zjh.DataPreProcessing.driver;

import com.zjh.DataPreProcessing.mapper.DataProcessingMapper;
import com.zjh.GenderClassification.util.JarUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataProcessing extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getMyConfiguration();
		conf.set("SPLITTER", args[2]);
		Job job=Job.getInstance(conf, "dataprocessing");
		job.setJarByClass(DataProcessing.class);
		job.setMapperClass(DataProcessingMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?-1:1;
	}
	public static void main(String[] args) {
		String[] myArgs={
				"/movie/gender_genre/part-r-00000",
				"/movie/processing_out",
				","	
		};
		try {
			ToolRunner.run(getMyConfiguration(), new DataProcessing(), myArgs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	public static Configuration getMyConfiguration(){
		//��������
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.app-submission.cross-platform",true);
		conf.set("fs.defaultFS", "hdfs://master:8020");// ָ��namenode
		conf.set("mapreduce.framework.name","yarn"); // ָ��ʹ��yarn���
		String resourcenode="master";
		conf.set("yarn.resourcemanager.address", resourcenode+":8032"); // ָ��resourcemanager
		conf.set("yarn.resourcemanager.scheduler.address",resourcenode+":8030");// ָ����Դ������
		conf.set("mapreduce.jobhistory.address",resourcenode+":10020");
		conf.set("mapreduce.job.jar",JarUtil.jar(DataProcessing.class));
		return conf;	
	}
}
