package sar.inputformats;

import sar.SarKey;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.Path;

import sar.recordreaders.*;
/** 
 * Reads the file as a whole bulk.
 * 
 */
public class BulkFileInputFormat extends FileInputFormat<SarKey, BytesWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<SarKey, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new BulkFileRecordReader();
	}
}
