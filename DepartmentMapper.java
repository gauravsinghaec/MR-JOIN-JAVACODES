package without.cache;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import custom.writable.TextPair;

public class DepartmentMapper extends Mapper<LongWritable,Text,TextPair,Text>{
	
	protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String str[] = value.toString().split("\t");
		context.write(new TextPair(str[0],"0"), new Text(str[1]));
	}
}
