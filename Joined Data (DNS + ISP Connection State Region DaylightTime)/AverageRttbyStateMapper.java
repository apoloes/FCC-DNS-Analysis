// mapper function for application to compute join of
// ADU file with file containing application names
// for system ports (port numbers < 1024).  The key for
// the join operation is the port number.
// this mapper handles the input SystemPorts file
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageRttbyStateMapper
  extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");

    String rtt = tokens[0];
    // String state = tokens[3];
    String state = tokens[3];

        // output the key, value pairs where the key is the unit ID
        // and the value is the line with an R prefix
	 context.write(new Text(state), new Text(rtt)); 
  }
}

