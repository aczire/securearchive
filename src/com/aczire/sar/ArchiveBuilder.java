package com.aczire.sar;

import java.io.IOException;
import java.security.MessageDigest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.aczire.sar.compression.GZipLib;
import com.aczire.sar.inputformats.*;
import com.aczire.sar.security.AESCrypter;


/**
 * @author 721917
 *
 */
public class ArchiveBuilder {
	private static final Log LOG = LogFactory.getLog(ArchiveBuilder.class);
	private String inPath = "";
	private String outPath = "";
	private String unlockKey = "KEY"; // Shell password to encrypt the blocks.
	private boolean compress = false;
	private boolean encrypt = false;

	private boolean inPathTypeLocal = false; // input path is hdfs.
	private boolean outPathTypeLocal = false; // output path is hdfs.

	Configuration conf = new Configuration();

	static class SequenceFileMapper
	extends Mapper<SarKey, BytesWritable, SarKey, BytesWritable> {
		public void map(SarKey key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String filename = key.Filename.toString();
			
			String sarKey = context.getConfiguration().get("sar.encrypt.key");			
			boolean compressFiles = Boolean.parseBoolean(context.getConfiguration().get("sar.compress"));
			boolean encryptFiles = Boolean.parseBoolean(context.getConfiguration().get("sar.encrypt"));
			
//			String sarOutputPath = context.getConfiguration().get("sar.out.path");
//			boolean sarOutPathLocal = Boolean.parseBoolean(context.getConfiguration().get("sar.out.path.local"));
			
			byte[] plainText = value.copyBytes();// getBytes() wont work, it seems there is a bug in the implementation.
			LOG.info("File " + filename + " plaintext Length: " + plainText.length);
			try {
				byte[] compressed;
				key.Compressed = compressFiles;
				compressed = (compressFiles) ? GZipLib.compress(plainText) : plainText;
				
				byte[] cipherText;
				key.Locked = encryptFiles;
				cipherText = (encryptFiles) ? AESCrypter.encrypt(compressed, sarKey) : compressed;
				
				LOG.info("File " + filename + " compressed. Length: " + compressed.length);
				LOG.info("File " + filename + " encrypted. Length: " + cipherText.length);

				MessageDigest md = MessageDigest.getInstance("sha-256");
				byte[] digestOfPassword = md.digest(sarKey.getBytes("utf-8"));

				key.Key = new String(digestOfPassword);

				value.setSize(cipherText.length);
				value.set(cipherText, 0, cipherText.length);
				context.write(key, value);
			} catch (Exception e) {
				LOG.error("Failed to archive " + filename + " " +  e.toString());
			}
		}
	}

	
	/**
	 * @param opts
	 */
	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("SecureArchive", opts);
	}	

	/**
	 * @param args
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 */
	public boolean init(String[] args) throws ParseException, IOException {
		LOG.info("Initializing ArchiveBuilder Client");
			
		Options opts = new Options();
		opts.addOption("in_path", true, "Input directory with files.");
		opts.addOption("in_path_local", false, "Specifies the input directory is local filesystem.");
		opts.addOption("out_path", true, "Output directory.");
		opts.addOption("out_path_local", false, "Specifies the output directory is local filesystem.");
		opts.addOption("key", true, "Password used to encrypt files.");
		opts.addOption("compress", false, "Search for the file <filename>. (case sensitive)");
		opts.addOption("encrypt", false, "Search for the keyword <keyword>.");
		opts.addOption("help", false, "Print usage information.");

		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException("No args specified for SecureArchive to initialize");
		}		

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			return false;
		}

		/*conf.set("mapreduce.output.fileoutputformat.compress", "true");
			conf.set("mapreduce.output.fileoutputformat.compression.type", "BLOCK");
			conf.set("mapreduce.output.fileoutputformat.compress.codec",
		    "org.apache.hadoop.io.compress.GzipCodec");*/
		if (!cliParser.hasOption("in_path")) {
			throw new IllegalArgumentException("No input folder specified.");
		}
		else {
			inPath = cliParser.getOptionValue("in_path");
			if (cliParser.hasOption("in_path_local")) {
				inPathTypeLocal = true;
			}			
		}
		if (!cliParser.hasOption("out_path")) {
			throw new IllegalArgumentException("No output folder specified.");
		}
		else {
			outPath = cliParser.getOptionValue("out_path");
			FileSystem fs = FileSystem.get(conf);
			Path outDirectory = new Path(outPath);
			if(fs.exists(outDirectory)){
				fs.delete(outDirectory, true);
			}
			
			if (cliParser.hasOption("out_path_local")) {
				outPathTypeLocal = true;
			}			
		}		

		if (cliParser.hasOption("compress")) {
			compress = true;
		}
		if (cliParser.hasOption("encrypt")) {
			encrypt = true;
		}
		/*if (!cliParser.hasOption("search_file") && !cliParser.hasOption("search_keyword")) {
			throw new IllegalArgumentException("Please specify either search keyword or file to search for.");
		}*/
		if (cliParser.hasOption("key")) {
			unlockKey = cliParser.getOptionValue("key");
		}

		conf.set("sar.out.path.local", Boolean.toString(outPathTypeLocal));
		conf.set("sar.out.path", outPath);		
		conf.set("sar.in.path.local", Boolean.toString(inPathTypeLocal));
		conf.set("sar.in.path", inPath);
		conf.set("sar.encrypt", Boolean.toString(encrypt));
		conf.set("sar.compress", Boolean.toString(compress));
		conf.set("sar.encrypt.key", unlockKey);		
		return true;
	}

	public boolean run() throws IOException, InterruptedException, ClassNotFoundException {
		LOG.info("Starting Client");	
		Job job = new Job(conf);
		job.setJarByClass(ArchiveBuilder.class);
		job.setJobName("SecureArchiver");
		if(inPathTypeLocal){
			LocalFileInputFormat.setInputPaths(job, new Path(inPath));
			job.setInputFormatClass(LocalBulkFileInputFormat.class);
		}
		else{
			FileInputFormat.setInputPaths(job, new Path(inPath));
			job.setInputFormatClass(BulkFileInputFormat.class);			
		}
		
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		/*SequenceFileOutputFormat.setCompressOutput(job, true);
				SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
				SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);*/
		job.setOutputKeyClass(SarKey.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(Reducer.class);
		//job.setNumReduceTasks(0);
		return(job.waitForCompletion(true) ? true : false);
	}


	public static void main(String[] args) throws Exception {
		boolean result = false;
		try {
			ArchiveBuilder archiveBuilder = new ArchiveBuilder();
			LOG.info("Initializing Secure Archive");
			boolean doRun = archiveBuilder.init(args);
			if (!doRun) {
				System.exit(0);
			}
			result = archiveBuilder.run();
		} catch (Throwable t) {
			LOG.fatal("Error running Client", t);
			System.exit(1);
		}
		if (result) {
			LOG.info("SecureArchive completed successfully");
			System.exit(0);			
		} 
		LOG.error("SecureArchive failed to complete successfully");
		System.exit(2);
	}
}
