import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by kk on 1/18/2017.
 */
public class TrackReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable trackId,
                       Iterable<Text> trackSpecificDatas,
                       Reducer<IntWritable, Text, IntWritable,
                               Text>.Context context)
            throws IOException, InterruptedException {
        int userIdCount = 0;
        int shareCount = 0;
        int radioCount = 0;
        int skipCount = 0;
        for (Text trackSpecificData : trackSpecificDatas) {
            String[] columns = trackSpecificData.toString().split("[|]");
            userIdCount += Integer.parseInt(columns[0]);
            shareCount += Integer.parseInt(columns[1]);
            radioCount += Integer.parseInt(columns[2]);
            skipCount += Integer.parseInt(columns[3]);
        }

        Text output = new Text();
        StringBuilder sb = new StringBuilder();
        sb.append(userIdCount);
        sb.append("|");
        sb.append(shareCount);
        sb.append("|");
        sb.append(radioCount);
        sb.append("|");
        sb.append(skipCount);
        output.set(sb.toString());

        context.write(trackId, output);
    }
}

