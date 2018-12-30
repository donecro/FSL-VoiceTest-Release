package com.zjh.DataPreProcessing.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.zjh.DataChange.UserAndGender;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// 对每个用户看过的电影类型进行统计
public class MoviesGenresReducer extends Reducer<UserAndGender, Text, Text, NullWritable> {
	@Override
	protected void reduce(UserAndGender key, Iterable<Text> value,
			Reducer<UserAndGender, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		//初始化一个HashMap集合，集合在的键为18种电影类型，每个键对应的值为0
		HashMap<String,Integer> genresCounts=new HashMap<String,Integer>();
		String[] genreslist={"Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama",
				"Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"		
				};
	    for(int i=0;i<genreslist.length;i++){
			if(!genresCounts.containsKey(genreslist[i])){
				genresCounts.put(genreslist[i], 0);
				}
			}
	    //遍历值列表
		for (Text val : value) {
			//对每个元素进行分隔
			String[] genres=val.toString().split("\\|");
			for(int i=0;i<genres.length;i++){
				//如果HashMap元素的键包含分隔结果的元素，则该键对应的值加1
				if(genresCounts.containsKey(genres[i])){
				   genresCounts.put(genres[i], genresCounts.get(genres[i])+1);
				}
			}
		}
		//将HashMap集合中所有键对应的值根据逗号连接成字符串
		String result="";
		for(Map.Entry<String, Integer> kv:genresCounts.entrySet()){
			if(result.length()==0){
				result=kv.getValue().toString();
			}else{
				result=result+","+kv.getValue();
			}
		}
		
	    context.write(new Text(key.toString()+","+result), NullWritable.get());
	}

}
