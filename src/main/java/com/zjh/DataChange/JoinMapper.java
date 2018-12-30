package com.zjh.DataChange;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private HashMap<String,String> user_info=new HashMap<String,String>();
	private String splitter="";
	private String rating_secondPart="";
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		splitter=context.getConfiguration().get("SPLITTER");
		Path[] distributePaths=DistributedCache.getLocalCacheFiles(context.getConfiguration());
		String line="";
		BufferedReader br=null;
		for (Path path : distributePaths) {
			if(path.toString().endsWith("users.dat")){
				br=new BufferedReader(new FileReader(path.toString()));
				while((line=br.readLine())!=null){
					String userID=line.substring(0, line.indexOf("::"));
					String secondPart=line.substring(line.indexOf("::")+2,line.length());
					user_info.put(userID, secondPart);
				}
			}
		}	
	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] val=value.toString().split(splitter);
		rating_secondPart=user_info.get(val[0]);
		if(rating_secondPart!=null){
			String result=val[0]+splitter+rating_secondPart+splitter+val[1];
			context.write(new Text(result), NullWritable.get());
		}
	}

}
