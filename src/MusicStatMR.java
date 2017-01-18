import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by kk on 1/18/2017.
 */
public class MusicStatMR {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        org.apache.hadoop.mapreduce.Job job = new org.apache.hadoop.mapreduce.Job(conf, "Music Stats");
        job.setJarByClass(MusicStatMR.class);
        job.setMapperClass(TrackMapper.class);
        job.setReducerClass(TrackReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


