
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner


class CountGroovyJob extends Configured implements Tool {
    @Override
    int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "StartsWithCount")
        job.setJarByClass(getClass())

        // configure output and input source
        TextInputFormat.addInputPath(job, new Path(args[0]))
        job.setInputFormatClass(TextInputFormat)

        // configure mapper and reducer
        job.setMapperClass(StartsWithCountMapper)
        job.setCombinerClass(StartsWithCountReducer)
        job.setReducerClass(StartsWithCountReducer)

        // configure output
        TextOutputFormat.setOutputPath(job, new Path(args[1]))
        job.setOutputFormatClass(TextOutputFormat)
        job.setOutputKeyClass(Text)
        job.setOutputValueClass(IntWritable)

        return job.waitForCompletion(true) ? 0 : 1
    }

    static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CountGroovyJob(), args))
    }

    class GroovyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable countOne = new IntWritable(1);
        private final Text reusableText = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) {
            value.toString().tokenize().each {
                reusableText.set(it)
                context.write(reusableText,countOne)
            }
        }
    }

    class GroovyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable outValue = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) {
        def sum=0
        values.each{
         sum+=it.get()
         }
           outValue.set(sum)
            context.write(key, outValue);
        }
    }
}