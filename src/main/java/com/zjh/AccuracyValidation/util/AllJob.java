package com.zjh.AccuracyValidation.util;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.zjh.AccuracyValidation.driver.Validate;
import com.zjh.GenderClassification.driver.MovieClassification;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

// 选择最优K值
public class AllJob {

	public static void main(String[] args) throws IOException {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "10.2.100.101:8020");
		FileSystem fs=FileSystem.get(conf);
		double maxAccuracy=0.0;
		int bestK=0;
		int[] k={2,3,5,9,15,30,55,70,80,100};
		for(int i=2;i<k.length;i++){
			double accuracy=0.0;
			String[] classifyArgs={
					"/movie/ValidateData",
					"/movie/TrainData",
					"/movie/KNN",
					String.valueOf(k[i]),
					","
			};
			try {
				ToolRunner.run(MovieClassification.getMyConfiguration(), new MovieClassification(), classifyArgs);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String[] validateArgs={
					"/movie/knnout/part-r-00000",
					"/movie/validateout",
					","
			};
			try {
				ToolRunner.run(Validate.getMyConfiguration(),new Validate(),validateArgs);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			FSDataInputStream is=fs.open(new Path("/movie/validateout/part-r-00000"));
			BufferedReader br=new BufferedReader(new InputStreamReader(is));
			String line="";
			while((line=br.readLine())!=null){
				accuracy=Double.parseDouble(line);
			}
			br.close();
			is.close();
			if(accuracy>maxAccuracy){
				maxAccuracy=accuracy;
				bestK=k[i];
			}
		}
		System.out.println("最优K值是："+bestK+"\t"+"最优K值对应的准确率："+maxAccuracy);
	}
}

