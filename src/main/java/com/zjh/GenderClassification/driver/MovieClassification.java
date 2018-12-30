package com.zjh.GenderClassification.driver;
import com.zjh.GenderClassification.util.DistanceAndLabel;
import com.zjh.GenderClassification.mapper.MovieClassifyMapper;
import com.zjh.GenderClassification.reducer.MovieClassifyReducer;
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

// 实现用户性别分类
public class MovieClassification extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        if(args.length!=5){
            System.err.println("demo.MovieClassify <testinput> <traininput> <output> <k> <splitter>");
            System.exit(-1);
        }
        Configuration conf=getMyConfiguration();
        conf.setInt("K", Integer.parseInt(args[3]));
        conf.set("SPLITTER",args[4]);
        conf.set("TESTPATH", args[0]);
        Job job=Job.getInstance(conf, "movie_knn");
        job.setJarByClass(MovieClassification.class);//设置主类
        job.setMapperClass(MovieClassifyMapper.class);//设置Mapper类
        job.setReducerClass(MovieClassifyReducer.class);//设置Reducer类
        job.setMapOutputKeyClass(Text.class);//设置Mapper输出的键类型
        job.setMapOutputValueClass(DistanceAndLabel.class);//设置Mapper输出的值类型
        job.setOutputKeyClass(Text.class);//设置Reducer输出的键类型
        job.setOutputValueClass(NullWritable.class);//设置Reducer输出的值类型
        FileInputFormat.addInputPath(job, new Path(args[1]));//设置输入路径
        FileSystem.get(conf).delete(new Path(args[2]), true);//删除输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[2]));//设置输出路径
        return job.waitForCompletion(true)?-1:1;//提交任务
    }
    public static void main(String[] args) {
        String[] myArgs={
                "/movie/testData",
                "/movie/trainData",
                "/movie/knnout",
                "3",
                ","
        };
        try {
            ToolRunner.run(getMyConfiguration(), new MovieClassification(), myArgs);
        } catch (Exception e) {
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
        conf.set("mapreduce.job.jar",JarUtil.jar(MovieClassification.class));
        return conf;
    }
}
