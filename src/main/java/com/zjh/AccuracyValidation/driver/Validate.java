package com.zjh.AccuracyValidation.driver;
import com.zjh.AccuracyValidation.mapper.ValidateMapper;
import com.zjh.AccuracyValidation.reducer.ValidateReducer;
import com.zjh.GenderClassification.util.JarUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// 分类评价
public class Validate extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=3){
			System.err.println("demo01.Validate <input> <output> <splitter>");
			System.exit(-1);
		}
		Configuration conf=getMyConfiguration();
		conf.set("SPLITTER", args[2]);
		Job job=Job.getInstance(conf, "validate");
		job.setJarByClass(Validate.class);//设置主类
		job.setMapperClass(ValidateMapper.class);//设置Mapper类
		job.setReducerClass(ValidateReducer.class);//设置Reducer类
		job.setMapOutputKeyClass(NullWritable.class);//设置Mapper输出的键格式
		job.setMapOutputValueClass(Text.class);//设置Mapper输出的值格式
		job.setOutputKeyClass(DoubleWritable.class);//设置Reducer输出的键格式
		job.setOutputValueClass(NullWritable.class);//设置Reducer输出的值格式
		FileInputFormat.addInputPath(job, new Path(args[0]));//设置输入路径
		FileSystem.get(conf).delete(new Path(args[1]),true);//设置删除输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//设置输出路径
		return job.waitForCompletion(true)?-1:1;
	}
	public static void main(String[] args) {
		String[] myArgs={
				"/movie/knnout/part-r-00000",
				"/movie/validateout",
				","
		};
		try {
			ToolRunner.run(getMyConfiguration(), new Validate(), myArgs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 设置连接Hadoop集群的配置
	 * @return
	 */
	public static Configuration getMyConfiguration(){
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.app-submission.cross-platform",true);
		conf.set("fs.defaultFS", "hdfs://master:8020");// 指定namenode
		conf.set("mapreduce.framework.name","yarn"); // 指定使用yarn框架
		String resourcenode="master";
		conf.set("yarn.resourcemanager.address", resourcenode+":8032"); // 指定resourcemanager
		conf.set("yarn.resourcemanager.scheduler.address",resourcenode+":8030");// 指定资源分配器
		conf.set("mapreduce.jobhistory.address",resourcenode+":10020");
		conf.set("mapreduce.job.jar",JarUtil.jar(Validate.class));
		return conf;
	}
}
