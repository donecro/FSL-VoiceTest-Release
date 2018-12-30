package com.zjh.AccuracyValidation.reducer;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 分类评价
public class ValidateReducer extends Reducer<NullWritable, Text, DoubleWritable, NullWritable> {
	private String splitter="";
	@Override
	protected void setup(Reducer<NullWritable, Text, DoubleWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		splitter=context.getConfiguration().get("SPLITTER");
	}
	@Override
	protected void reduce(NullWritable key, Iterable<Text> value,
						  Reducer<NullWritable, Text, DoubleWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		//初始化sum记录预测分类正确的个数
		int sum=0;
		//初始化count记录所有分类结果的记录数，也即测试数据的记录数
		int count=0;
		for (Text val: value) {
			count++;
			String predictLabel=val.toString().split(splitter)[0];
			String trueLabel=val.toString().split(splitter)[1];
			//判断预测分类的类别是否与正确分类的类别一样
			if(predictLabel.equals(trueLabel)){
				sum+=1;
			}
		}
		//计算正确率
		double accuracy=(double)sum/count;
		context.write(new DoubleWritable(accuracy), NullWritable.get());
	}
}