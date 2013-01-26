package com.aczire.sar.inputformats;


import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.Path;

import com.aczire.sar.SarKey;
import com.aczire.sar.recordreaders.*;
/** 
 * The implementation class for local file-based {@link InputFormat}s.
 * 
 * <p><code>LocalBulkFileInputFormat</code> is the implementation class for all local file-based 
 * <code>InputFormat</code>s.
 * Subclasses of <code>LocalBulkFileInputFormat</code> overrides the 
 * {@link #isSplitable(JobContext, Path)} method to ensure input-files are
 * not split-up and are processed as a whole by {@link Mapper}s.
 */
public class LocalBulkFileInputFormat extends LocalFileInputFormat<SarKey, BytesWritable> {

	/**
	 * Overrides the {@link #isSplitable(JobContext, Path)} method 
	 * to ensure input-files are not split-up and are processed 
	 * as a whole by {@link Mapper}s.
	 * 
	 * @see sar.LocalFileInputFormat#isSplitable(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	/**
	 * Create the <p><code>LocalBulkFileRecordReader to read the files 
	 * and return each file as an {@link InputSplit}</code>
	 */
	@Override
	public RecordReader<SarKey, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new LocalBulkFileRecordReader();
	}
}
