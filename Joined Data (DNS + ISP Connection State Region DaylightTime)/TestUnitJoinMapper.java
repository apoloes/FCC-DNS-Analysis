// mapper function for application to compute join of
// ADU file with file containing application names
// for system ports (port numbers < 1024).  The key for
// the join operation is the port number.
// this mapper handles the input ADU file
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TestUnitJoinMapper
  extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    
    String isp = tokens[1];
    String connection = tokens[2];
    String state = tokens[3];
    String region = tokens[4];
    String daylight_time = tokens[6];
    
    String line2 = isp + "," + connection + "," + state + "," + region + "," + daylight_time;

        // output the key, value pairs where the key is the unit ID 
        // and the value is the state with an S prefix
	context.write(new Text(tokens[0]), new Text("S" + line2));
  }
}

