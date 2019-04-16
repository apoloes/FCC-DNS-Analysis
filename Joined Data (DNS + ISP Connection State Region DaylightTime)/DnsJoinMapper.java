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

public class DnsJoinMapper
  extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");

    String unitID = tokens[0].substring(1,tokens[0].length()-1);
    String dtime = tokens[1].substring(1, tokens[1].length()-1);
    String nameserver = tokens[2].substring(1, tokens[2].length()-1);
    String lookup_host = tokens[3].substring(1, tokens[3].length()-1);
    String response_ip = tokens[4].substring(1, tokens[4].length()-1);
    String rtt = tokens[5].substring(1, tokens[5].length()-1);
    String successes = tokens[6].substring(1, tokens[6].length()-1);

    String line_noquotes = unitID + "," + dtime + "," + nameserver + "," + lookup_host + "," + response_ip + "," + rtt + "," + successes;

        // output the key, value pairs where the key is the unit ID
        // and the value is the line with an R prefix
	 context.write(new Text(unitID), new Text("R" + line_noquotes));
  }
}

