package com.zjh.DataPreProcessing.mapper;

import java.io.IOException;

import com.zjh.DataChange.UserAndGender;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// 对每个用户看过的电影类型进行统计
public class MoviesGenresMapper extends Mapper<LongWritable, Text, UserAndGender, Text> {
	private UserAndGender user_gender=new UserAndGender();
	private String splitter="";
	private Text genres=new Text();
	@Override
	protected void setup(Mapper<LongWritable, Text, UserAndGender, Text>.Context context)
			throws IOException, InterruptedException {
		splitter=context.getConfiguration().get("SPLITTER");
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, UserAndGender, Text>.Context context)
			throws IOException, InterruptedException {
		String[] val=value.toString().split(splitter);
		user_gender.setUserID(val[0]);
		if(val[1].equals("M")){
			//性别为M则用0标记
			user_gender.setGender(0);
		}else{
			//性别为F则用1标记
			user_gender.setGender(1);
		}
		user_gender.setAge(Integer.parseInt(val[2]));
		user_gender.setOccupation(val[3]);
		user_gender.setZip_code(val[4]);
		genres.set(val[6]);
		context.write(user_gender, genres);
	}
}
