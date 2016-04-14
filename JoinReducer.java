package without.cache;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import custom.writable.TextPair;

public class JoinReducer extends Reducer<TextPair,Text,Text,Text>{
	
	protected void reduce(TextPair key,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		Iterator<Text> itr = values.iterator();
		Text deptName = new Text(itr.next());
		while(itr.hasNext())
		{
			Text rec = itr.next();
			String outValue = deptName.toString() +"\t"+rec.toString();
			context.write(key.getFirst(), new Text(outValue));
		}
		
	}
}
