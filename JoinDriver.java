package without.cache;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import custom.writable.FirstComparator;
import custom.writable.TextPair;

public class JoinDriver {

	public static void main(String[] arg) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"MapReduce join");
		
		job.setJarByClass(JoinDriver.class);
		
		MultipleInputs.addInputPath(job,new Path(arg[0]),TextInputFormat.class,DepartmentMapper.class);
		MultipleInputs.addInputPath(job,new Path(arg[1]),TextInputFormat.class,EmployeeMapper.class);
		
		FileOutputFormat.setOutputPath(job,new Path(arg[2]));
		
		job.setPartitionerClass(JoinPartitioner.class);
		job.setGroupingComparatorClass(FirstComparator.class);
		
		job.setMapOutputKeyClass(TextPair.class);
		
		job.setReducerClass(JoinReducer.class);
		
		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
