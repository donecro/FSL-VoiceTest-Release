package com.zjh.DataChange;


import com.zjh.GenderClassification.util.JarUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// 连接movies.dat数据和ratings_users.dat数据

public class UsersAndMovies extends Configured implements Tool {
	public static Configuration getMyConfiguration(){
		//声明配置
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.app-submission.cross-platform",true);
		conf.set("fs.defaultFS", "hdfs://master:8020");// 指定namenode
		conf.set("mapreduce.framework.name","yarn"); // 指定使用yarn框架
		String resourcenode="master";
		conf.set("yarn.resourcemanager.address", resourcenode+":8032"); // 指定resourcemanager
		conf.set("yarn.resourcemanager.scheduler.address",resourcenode+":8030");// 指定资源分配器
		conf.set("mapreduce.jobhistory.address",resourcenode+":10020");
		conf.set("mapreduce.job.jar",JarUtil.jar(UsersAndMovies.class));
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=4){
			System.err.println("demo.RatingsAndUsers <cachePath> <input> <output> <splitter>");
			System.exit(-1);
		}
		Configuration conf=UsersAndMovies.getMyConfiguration();
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
		conf.set("SPLITTER", args[3]);
		Job job=Job.getInstance(conf, "joindata");
		job.setJarByClass(UsersAndMovies.class);
		job.setMapperClass(UsersMoviesMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileSystem.get(conf).delete(new Path(args[2]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true)?-1:1;
	}
	public static void main(String[] args) {
		String[] myArgs={
				"/movie/movies.dat",
				"/movie/ratings_users/part-m-00000",
				"/movie/users_movies",
				"::"
		};
		try {
			ToolRunner.run(getMyConfiguration(), new UsersAndMovies(), myArgs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

