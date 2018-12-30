package com.zjh.GenderClassification.mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import com.zjh.GenderClassification.util.DistanceAndLabel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// 实现用户性别分类
public class MovieClassifyMapper extends Mapper<LongWritable, Text, Text, DistanceAndLabel> {
	private DistanceAndLabel distance_label=new DistanceAndLabel();
	private String splitter="";
	ArrayList<String> testData=new ArrayList<String>();
	private String testPath="";
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DistanceAndLabel>.Context context) throws IOException {
		Configuration conf=context.getConfiguration();
		splitter=conf.get("SPLITTER");
		testPath=conf.get("TESTPATH");
		//读取测试数据存于列表TestData中
		FileSystem fs=FileSystem.get(conf);
		FSDataInputStream is=fs.open(new Path(testPath));
		BufferedReader br=new BufferedReader(new InputStreamReader(is));
		String line="";
		while((line=br.readLine())!=null){
			testData.add(line);
		}
		is.close();
		br.close();
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DistanceAndLabel>.Context context)
			throws IOException, InterruptedException {
		double distance=0.0;
		String[] val=value.toString().split(splitter);
		String[] singleTrainData=Arrays.copyOfRange(val, 5, val.length);
		String label=val[1];
		for (String td: testData) {
			String[] test=td.split(splitter);
			String[] singleTestData=Arrays.copyOfRange(test, 5, test.length);
			distance=Distance(singleTrainData,singleTestData);
			distance_label.setDistance(distance);
			distance_label.setLabel(label);
			context.write(new Text(td), distance_label);
		}
	}
	/**
	 * 计算训练数据与测试数据的距离
	 * @param singleTrainData
	 * @param singleTestData
	 * @return
	 */
	private double Distance(String[] singleTrainData, String[] singleTestData) {
		double sum=0.0;
		for(int i=0;i<singleTrainData.length;i++){
			sum+=Math.pow(Double.parseDouble(singleTrainData[i]), Double.parseDouble(singleTestData[i]));
		}
		return Math.sqrt(sum);
	}
}