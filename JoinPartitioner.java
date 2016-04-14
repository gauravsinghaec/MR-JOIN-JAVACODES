package without.cache;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import custom.writable.TextPair;

public class JoinPartitioner extends Partitioner<TextPair,Text>{

	@Override
	public int getPartition(TextPair key, Text value, int numReduceTasks) {
		// TODO Auto-generated method stub
		return (key.getFirst().hashCode() & Integer.MAX_VALUE)% numReduceTasks;
	}

}
