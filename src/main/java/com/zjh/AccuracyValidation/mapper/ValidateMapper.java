package com.zjh.AccuracyValidation.mapper;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// 分类评价
public class ValidateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String splitter="";
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		splitter=context.getConfiguration().get("SPLITTER");
	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] val=value.toString().split(splitter);
		context.write(NullWritable.get(), new Text(val[0]+splitter+val[2]));
	}
}
