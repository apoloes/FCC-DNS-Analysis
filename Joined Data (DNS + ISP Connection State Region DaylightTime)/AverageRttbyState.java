// main function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageRttbyState {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: AverageRttbyState <input path> <output path>");
      System.exit(-1);
    }
    //define the job to the JobTracker
    Job job = Job.getInstance();
    job.setJarByClass(AverageRttbyState.class);
    job.setJobName("Average Rtt by State");

    // set the input and output paths (passed as args)
    // MultipleInputs.addInputPath(job, new Path(args[0]), 
    //            TextInputFormat.class, DnsJoinMapper.class);
    // MultipleInputs.addInputPath(job, new Path(args[1]), 
    //            TextInputFormat.class, TestUnitJoinMapper.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(AverageRttbyStateMapper.class);
    job.setReducerClass(AverageRttbyStateReducer.class);

    // set the format of the keys and values
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // set the number of reduce tasks
    job.setNumReduceTasks(10);

    // submit the job and wait for its completion    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
