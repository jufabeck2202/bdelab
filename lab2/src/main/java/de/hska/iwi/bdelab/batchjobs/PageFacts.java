
package de.hska.iwi.bdelab.batchjobs;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

import de.hska.iwi.bdelab.batchstore.FileUtils;
import de.hska.iwi.bdelab.schema2.Data;
import manning.tap2.DataPailStructure;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import com.backtype.hadoop.pail.PailFormat;
import com.backtype.hadoop.pail.PailFormatFactory;
import com.backtype.hadoop.pail.PailSpec;

public class PageFacts {

	public static class Map extends MapReduceBase implements Mapper<Text, BytesWritable, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static Text word = new Text();

		private transient TDeserializer des;

		private TDeserializer getDeserializer() {
			if (des == null)
				des = new TDeserializer();
			return des;
		}

		// helper method for deserializing de.hska.iwi.bdelab.schema2.Data objects (aka
		// facts)
		public Data deserialize(byte[] record) {
			Data ret = new Data();
			try {
				getDeserializer().deserialize((TBase) ret, record);
			} catch (TException e) {
				throw new RuntimeException(e);
			}
			return ret;
		}

		// THE MAP FUNCTION
		public void map(Text key, BytesWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			Data data = deserialize(value.getBytes());
			long dateInSec = data.get_pedigree().get_true_as_of_secs() * 1000L;
			String page = data.get_dataunit().get_pageview().get_page().get_url();
			SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.YYYY - HH");
			Date date = new Date();
			date.setTime(dateInSec);
			System.out.println(date);
			String timestamp = sdf.format(date);
			System.out.println(timestamp);
			word.set(timestamp + "  " + page);

			// a static key results in a single partition on the reducer-side
			output.collect(word, one);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		// THE REDUCE FUNCTION
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PageFacts.class);
		conf.setJobName("page-count");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		////////////////////////////////////////////////////////////////////////////
		// input as pails
		PailSpec spec = PailFormatFactory.getDefaultCopy().setStructure(new DataPailStructure());
		PailFormat format = PailFormatFactory.create(spec);
		String masterPath = FileUtils.prepareMasterFactsPath(false, false);
		//
		conf.setInputFormat(format.getInputFormatClass());
		FileInputFormat.setInputPaths(conf, new Path(masterPath));
		////////////////////////////////////////////////////////////////////////////

		////////////////////////////////////////////////////////////////////////////
		// output as text
		conf.setOutputFormat(TextOutputFormat.class);
		FileSystem fs = FileUtils.getFs(false);
		FileOutputFormat.setOutputPath(conf, new Path(FileUtils.getTmpPath(fs, "page-count", true, false)));
		////////////////////////////////////////////////////////////////////////////

		JobClient.runJob(conf);
	}
}
