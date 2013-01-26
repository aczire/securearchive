package com.aczire.sar.recordreaders;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.aczire.sar.SarKey;


public class LocalBulkFileRecordReader extends RecordReader<SarKey, BytesWritable> {

	private static final Log LOG = LogFactory.getLog(LocalBulkFileRecordReader.class);

	private FileSplit fileSplit;
	private boolean processed = false;

	private SarKey key = new SarKey(); 
	private BytesWritable value = new BytesWritable();

	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) inputSplit;
	}

	public boolean nextKeyValue() throws IOException {
		if (!processed) {
			byte[] contents = new byte[(int) fileSplit.getLength()];

			Path file = fileSplit.getPath();
			InputStream in = null;
			try {
				//in = fs.open(file);
				URL url = new URL (fileSplit.getPath().toUri().toString());
				in = url.openStream ();
				IOUtils.readFully(in, contents, 0, contents.length);

				LOG.info("File " + file.getName() + " read. Length: " + contents.length);

				key.Filename = file.getName();
				key.FileSize = contents.length;
				//value = new BytesWritable(contents, contents.length);
				value.setSize(contents.length);
				value.set(contents, 0, contents.length);
			} finally {
				//IOUtils.closeStream(in);
				in.close();
			}
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public SarKey getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException  {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}
}