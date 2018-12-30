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

public class UsersMoviesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private HashMap<String,String> movie_info=new HashMap<String,String>();
	private String splitter="";
	private String movie_secondPart="";
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Path[] DistributePaths=DistributedCache.getLocalCacheFiles(context.getConfiguration());
		splitter=context.getConfiguration().get("SPLITTER");
		String line="";
		BufferedReader br=null;
		for (Path path : DistributePaths) {
			if(path.toString().endsWith("movies.dat")){
			br=new BufferedReader(new FileReader(path.toString()));
			while((line=br.readLine())!=null){
				String movieID=line.split(splitter)[0];
				String genres=line.split(splitter)[2];
				movie_info.put(movieID, genres);
			}
		}
		}
	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] val=value.toString().split(splitter);
		movie_secondPart=movie_info.get(val[5]);
		if(movie_secondPart!=null){
			String result=value.toString()+splitter+movie_secondPart;
			context.write(new Text(result), NullWritable.get());
		}
	}

}
