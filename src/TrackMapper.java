import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static org.apache.hadoop.mapred.JobHistory.Keys.COUNTERS;

/**
 * Created by kk on 1/18/2017.
 */
public class TrackMapper extends Mapper<Object, Text, IntWritable, Text> {

    private IntWritable trackId = new IntWritable();
    private IntWritable userId = new IntWritable();
    private IntWritable share = new IntWritable();
    private IntWritable radio = new IntWritable();
    private IntWritable skip = new IntWritable();

    public void map(Object key, Text value,
                    Mapper<Object, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {

        String[] columns = value.toString().split("[|]");
        trackId.set(Integer.parseInt(columns[1]));
        String arr = "1" + "|" + columns[2] + "|" + columns[3] + "|" + columns[4];
        Text trackSpecificData = new Text();
        trackSpecificData.set(arr);
        if (columns.length == 5) {
            context.write(trackId, trackSpecificData);
        }
    }


}
