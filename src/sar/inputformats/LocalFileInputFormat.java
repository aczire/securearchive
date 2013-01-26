package sar.inputformats;

import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/** 
 * A base class for local file-based {@link InputFormat}s.
 * 
 * <p><code>LocalFileInputFormat</code> is the base class for all local file-based 
 * <code>InputFormat</code>s. This provides a generic implementation of
 * {@link #getSplits(JobContext)}.
 * Subclasses of <code>LocalFileInputFormat</code> can also override the 
 * {@link #isSplitable(JobContext, Path)} method to ensure input-files are
 * not split-up and are processed as a whole by {@link Mapper}s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class LocalFileInputFormat<K, V> extends InputFormat<K, V> {
	public static final String INPUT_DIR = 
			"mapreduce.input.fileinputformat.inputdir";
	public static final String SPLIT_MAXSIZE = 
			"mapreduce.input.fileinputformat.split.maxsize";
	public static final String SPLIT_MINSIZE = 
			"mapreduce.input.fileinputformat.split.minsize";
	public static final String PATHFILTER_CLASS = 
			"mapreduce.input.pathFilter.class";
	public static final String NUM_INPUT_FILES =
			"mapreduce.input.fileinputformat.numinputfiles";

	private static final Log LOG = LogFactory.getLog(LocalFileInputFormat.class);

	private static final double SPLIT_SLOP = 1.1;   // 10% slop

	/**
	 * We may need to enable input filtering later on.
	 */
	private static final PathFilter hiddenFileFilter = new PathFilter(){
		public boolean accept(Path p){
			String name = p.getName(); 
			return !name.startsWith("_") && !name.startsWith("."); 
		}
	};

	/**
	 * We may need to enable input filtering later on.
	 * Proxy PathFilter that accepts a path only if all filters given in the
	 * constructor do. Used by the listPaths() to apply the built-in
	 * hiddenFileFilter together with a user provided one (if any).
	 */
	private static class MultiPathFilter implements PathFilter {
		private List<PathFilter> filters;

		public MultiPathFilter(List<PathFilter> filters) {
			this.filters = filters;
		}

		public boolean accept(Path path) {
			for (PathFilter filter : filters) {
				if (!filter.accept(path)) {
					return false;
				}
			}
			return true;
		}
	}

	/**
	 * Get the lower bound on split size imposed by the format.
	 * @return the number of bytes of the minimal split for this format
	 */
	protected long getFormatMinSplitSize() {
		return 1;
	}

	/**
	 * Is the given filename splitable? Usually, true, but if the file is
	 * stream compressed, it will not be.
	 * 
	 * <code>FileInputFormat</code> implementations can override this and return
	 * <code>false</code> to ensure that individual input files are never split-up
	 * so that {@link Mapper}s process entire files.
	 * 
	 * @param context the job context
	 * @param filename the file name to check
	 * @return is this file splitable?
	 */
	protected boolean isSplitable(JobContext context, Path filename) {
		return true;
	}

	/**
	 * We may need to enable input filtering later on.
	 * Set a PathFilter to be applied to the input paths for the map-reduce job.
	 * @param job the job to modify
	 * @param filter the PathFilter class use for filtering the input paths.
	 */
	/*public static void setInputPathFilter(Job job,
			Class<? extends PathFilter> filter) {
		job.getConfiguration().setClass(PATHFILTER_CLASS, filter, 
				PathFilter.class);
	}*/

	/**
	 * Set the minimum input split size
	 * @param job the job to modify
	 * @param size the minimum size
	 */
	public static void setMinInputSplitSize(Job job,
			long size) {
		job.getConfiguration().setLong(SPLIT_MINSIZE, size);
	}

	/**
	 * Get the minimum split size
	 * @param job the job
	 * @return the minimum number of bytes that can be in a split
	 */
	public static long getMinSplitSize(JobContext job) {
		return job.getConfiguration().getLong(SPLIT_MINSIZE, 1L);
	}

	/**
	 * Set the maximum split size
	 * @param job the job to modify
	 * @param size the maximum split size
	 */
	public static void setMaxInputSplitSize(Job job,
			long size) {
		job.getConfiguration().setLong(SPLIT_MAXSIZE, size);
	}

	/**
	 * Get the maximum split size.
	 * @param context the job to look at.
	 * @return the maximum number of bytes a split can include
	 */
	public static long getMaxSplitSize(JobContext context) {
		return context.getConfiguration().getLong(SPLIT_MAXSIZE, 
				Long.MAX_VALUE);
	}

	/**
	 * Get a PathFilter instance of the filter set for the input paths.
	 *
	 * @return the PathFilter instance set for the job, NULL if none has been set.
	 */
	public static PathFilter getInputPathFilter(JobContext context) {
		Configuration conf = context.getConfiguration();
		Class<?> filterClass = conf.getClass(PATHFILTER_CLASS, null,
				PathFilter.class);
		return (filterClass != null) ?
				(PathFilter) ReflectionUtils.newInstance(filterClass, conf) : null;
	}

	public FileStatus[] listStatus(Path f) throws IOException {
		File localf = new File(f.toUri().getPath());
		FileStatus[] results;
		
		//LOG.info("Checking existance of " + localf.getAbsolutePath());
		//LOG.info("Path to file " + f.toUri().getPath());
		//LOG.info("File URL " + f.toUri().toURL());
		//LOG.info("Canonical path " + localf.getCanonicalPath());

		if (!localf.exists()) {
			//LOG.info("File doesnot exists.");
			throw new FileNotFoundException("File " + f + " does not exist.");
		}
		//LOG.info("File " + f + " does exists.");
		if (localf.isFile()) {
			return new FileStatus[] {
					new RawLocalFileStatus(localf, 32 * 1024 * 1024, new RawLocalFileSystem()) };
		}

		String[] names = localf.list();
		if (names == null) {
			return null;
		}
		results = new FileStatus[names.length];
		int j = 0;
		for (int i = 0; i < names.length; i++) {
			try {
				File path = new File(new Path(f, names[i]).toUri().getPath());
				results[j] = new RawLocalFileStatus(path, 32 * 1024 * 1024, new RawLocalFileSystem()); //getFileStatus(new Path(f, names[i]));
				j++;
			} catch (Exception e) {
				// ignore the files not found since the dir list may have have changed
				// since the names[] list was generated.
				LOG.error(e.toString());
			}
		}
		if (j == names.length) {
			return results;
		}
		return Arrays.copyOf(results, j);
	}


	/** List input directories.
	 * Subclasses may override to, e.g., select only files matching a regular
	 * expression. 
	 * 
	 * @param job the job to list input paths for
	 * @return array of FileStatus objects
	 * @throws IOException if zero items.
	 */
	protected List<FileStatus> listStatus(JobContext job
			) throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();
		Path[] dirs = getInputPaths(job);
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}

		// get tokens for all the required FileSystems..
		/*TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, 
                                        job.getConfiguration());*/

		List<IOException> errors = new ArrayList<IOException>();

		// We may need to enable input filter later, but leave it out for now.
		// creates a MultiPathFilter with the hiddenFileFilter and the
		// user provided one (if any).
		List<PathFilter> filters = new ArrayList<PathFilter>();
		filters.add(hiddenFileFilter);
		PathFilter jobFilter = getInputPathFilter(job);
		if (jobFilter != null) {
			filters.add(jobFilter);
		}
		PathFilter inputFilter = new MultiPathFilter(filters);

		for (int i=0; i < dirs.length; ++i) {
			Path p = dirs[i];
			p.getFileSystem(job.getConfiguration());
			//RawLocalFileSystem fs = new RawLocalFileSystem();// p.getFileSystem(job.getConfiguration());
			FileSystem fs = FileSystem.getLocal(job.getConfiguration());
			
			// Disable input filter for now, we need to build globstatus with input filter later. 
			//FileStatus[] matches = /*fs.listStatus(p, inputFilter);*/listStatus(p); //fs.globStatus(p, inputFilter);
			FileStatus[] matches = fs.listStatus(p, inputFilter);/*listStatus(p); //fs.globStatus(p, inputFilter);*/
			if (matches == null) {
				errors.add(new IOException("Input path does not exist: " + p));
			} else if (matches.length == 0) {
				errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
			} else {
				for (FileStatus globStat: matches) {
					if (globStat.isDirectory()) {
						for(FileStatus stat: fs.listStatus(globStat.getPath()/*, inputFilter*/)) {
							result.add(stat);
						}          
					} else {
						result.add(globStat);
					}
				}
			}
		}
		
		if (!errors.isEmpty()) {
			throw new InvalidInputException(errors);
		}
		LOG.info("Total input paths to process : " + result.size());
		return result;
	}

	/**
	 * A factory that makes the split for this class. It can be overridden
	 * by sub-classes to make sub-types
	 */
	protected FileSplit makeSplit(Path file, long start, long length, 
			String[] hosts) {
		return new FileSplit(file, start, length, hosts);
	}

	/** 
	 * Generate the list of files and make them into FileSplits.
	 * @param job the job context
	 * @throws IOException
	 */
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);
		Configuration conf = job.getConfiguration();

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file: files) {
			Path path = file.getPath();
			long length = file.getLen();
			if (length != 0) {
				//RawLocalFileSystem fs = new RawLocalFileSystem();// path.getFileSystem(job.getConfiguration());
				FileSystem fs = FileSystem.getLocal(conf);
				BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
				if (isSplitable(job, path)) {
					long blockSize = file.getBlockSize();
					long splitSize = computeSplitSize(blockSize, minSize, maxSize);

					long bytesRemaining = length;
					while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
						int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
						splits.add(makeSplit(path, length-bytesRemaining, splitSize,
								blkLocations[blkIndex].getHosts()));
						bytesRemaining -= splitSize;
					}

					if (bytesRemaining != 0) {
						int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
						splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
								blkLocations[blkIndex].getHosts()));
					}
				} else { // not splitable
					splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts()));
				}
			} else { 
				//Create empty hosts array for zero length files
				splits.add(makeSplit(path, 0, length, new String[0]));
			}
		}
		// Save the number of input files for metrics/loadgen
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		LOG.debug("Total # of splits: " + splits.size());
		return splits;
	}

	protected long computeSplitSize(long blockSize, long minSize,
			long maxSize) {
		return Math.max(minSize, Math.min(maxSize, blockSize));
	}

	protected int getBlockIndex(BlockLocation[] blkLocations, 
			long offset) {
		for (int i = 0 ; i < blkLocations.length; i++) {
			// is the offset inside this block?
			if ((blkLocations[i].getOffset() <= offset) &&
					(offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
				return i;
			}
		}
		BlockLocation last = blkLocations[blkLocations.length -1];
		long fileLength = last.getOffset() + last.getLength() -1;
		throw new IllegalArgumentException("Offset " + offset + 
				" is outside of file (0.." +
				fileLength + ")");
	}

	/**
	 * Sets the given comma separated paths as the list of inputs 
	 * for the map-reduce job.
	 * 
	 * @param job the job
	 * @param commaSeparatedPaths Comma separated paths to be set as 
	 *        the list of inputs for the map-reduce job.
	 */
	public static void setInputPaths(Job job, 
			String commaSeparatedPaths
			) throws IOException {
		setInputPaths(job, StringUtils.stringToPath(
				getPathStrings(commaSeparatedPaths)));
	}

	/**
	 * Add the given comma separated paths to the list of inputs for
	 *  the map-reduce job.
	 * 
	 * @param job The job to modify
	 * @param commaSeparatedPaths Comma separated paths to be added to
	 *        the list of inputs for the map-reduce job.
	 */
	public static void addInputPaths(Job job, 
			String commaSeparatedPaths
			) throws IOException {
		for (String str : getPathStrings(commaSeparatedPaths)) {
			addInputPath(job, new Path(str));
		}
	}

	/**
	 * Set the array of {@link Path}s as the list of inputs
	 * for the map-reduce job.
	 * 
	 * @param job The job to modify 
	 * @param inputPaths the {@link Path}s of the input directories/files 
	 * for the map-reduce job.
	 */ 
	public static void setInputPaths(Job job, 
			Path... inputPaths) throws IOException {
		Configuration conf = job.getConfiguration();
		//Path path = new LocalFileSystem().makeQualified(inputPaths[0]);
		Path path = FileSystem.getLocal(conf).makeQualified(inputPaths[0]);
		StringBuffer str = new StringBuffer(StringUtils.escapeString(path.toString()));
		for(int i = 1; i < inputPaths.length;i++) {
			str.append(StringUtils.COMMA_STR);
			path = inputPaths[i].getFileSystem(conf).makeQualified(inputPaths[i]);
			str.append(StringUtils.escapeString(path.toString()));
		}

		conf.set(INPUT_DIR, str.toString());
		//LOG.info("INPUT_DIR set to " + str.toString());
	}

	/**
	 * Add a {@link Path} to the list of inputs for the map-reduce job.
	 * 
	 * @param job The {@link Job} to modify
	 * @param path {@link Path} to be added to the list of inputs for 
	 *            the map-reduce job.
	 */
	public static void addInputPath(Job job, 
			Path path) throws IOException {
		Configuration conf = job.getConfiguration();
		path = path.getFileSystem(conf).makeQualified(path);
		String dirStr = StringUtils.escapeString(path.toString());
		String dirs = conf.get(INPUT_DIR);
		conf.set(INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
	}

	// This method escapes commas in the glob pattern of the given paths.
	private static String[] getPathStrings(String commaSeparatedPaths) {
		int length = commaSeparatedPaths.length();
		int curlyOpen = 0;
		int pathStart = 0;
		boolean globPattern = false;
		List<String> pathStrings = new ArrayList<String>();

		for (int i=0; i<length; i++) {
			char ch = commaSeparatedPaths.charAt(i);
			switch(ch) {
			case '{' : {
				curlyOpen++;
				if (!globPattern) {
					globPattern = true;
				}
				break;
			}
			case '}' : {
				curlyOpen--;
				if (curlyOpen == 0 && globPattern) {
					globPattern = false;
				}
				break;
			}
			case ',' : {
				if (!globPattern) {
					pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
					pathStart = i + 1 ;
				}
				break;
			}
			}
		}
		pathStrings.add(commaSeparatedPaths.substring(pathStart, length));

		return pathStrings.toArray(new String[0]);
	}

	/**
	 * Get the list of input {@link Path}s for the map-reduce job.
	 * 
	 * @param context The job
	 * @return the list of input {@link Path}s for the map-reduce job.
	 */
	public static Path[] getInputPaths(JobContext context) {
		String dirs = context.getConfiguration().get(INPUT_DIR, "");
		LOG.info("Input dirs " + dirs );
		String [] list = StringUtils.split(dirs);
		Path[] result = new Path[list.length];
		for (int i = 0; i < list.length; i++) {
			result[i] = new Path(StringUtils.unEscapeString(list[i]));
		}
		return result;
	}
	
	static class RawLocalFileStatus extends FileStatus {
		/* We can add extra fields here. It breaks at least CopyFiles.FilePair().
		 * We recognize if the information is already loaded by check if
		 * onwer.equals("").
		 */
		private boolean isPermissionLoaded() {
			return !super.getOwner().equals(""); 
		}

		RawLocalFileStatus(File f, long defaultBlockSize, FileSystem fs) {
			super(f.length(), f.isDirectory(), 1, defaultBlockSize,
					f.lastModified(), fs.makeQualified(new Path(f.getPath())));
		}

		@Override
		public FsPermission getPermission() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getPermission();
		}

		@Override
		public String getOwner() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getOwner();
		}

		@Override
		public String getGroup() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getGroup();
		}

		/// loads permissions, owner, and group from `ls -ld`
		private void loadPermissionInfo() {
			IOException e = null;
			try {
				StringTokenizer t = new StringTokenizer(
						execCommand(new File(getPath().toUri()), 
								Shell.getGET_PERMISSION_COMMAND()));
				//expected format
				//-rw-------    1 username groupname ...
				String permission = t.nextToken();
				if (permission.length() > 10) { //files with ACLs might have a '+'
					permission = permission.substring(0, 10);
				}
				setPermission(FsPermission.valueOf(permission));
				t.nextToken();
				setOwner(t.nextToken());
				setGroup(t.nextToken());
			} catch (Shell.ExitCodeException ioe) {
				if (ioe.getExitCode() != 1) {
					e = ioe;
				} else {
					setPermission(null);
					setOwner(null);
					setGroup(null);
				}
			} catch (IOException ioe) {
				e = ioe;
			} finally {
				if (e != null) {
					throw new RuntimeException("Error while running command to get " +
							"file permissions : " + 
							StringUtils.stringifyException(e));
				}
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			super.write(out);
		}
	}

	/**
	 * Use the command chown to set owner.
	 */
	public void setOwner(Path p, String username, String groupname)
			throws IOException {
		if (username == null && groupname == null) {
			throw new IOException("username == null && groupname == null");
		}

		if (username == null) {
			execCommand(new File(p.toUri().getPath()), Shell.SET_GROUP_COMMAND, groupname); 
		} else {
			//OWNER[:[GROUP]]
			String s = username + (groupname == null? "": ":" + groupname);
			execCommand(new File(p.toUri().getPath()), Shell.SET_OWNER_COMMAND, s);
		}
	}

	/**
	 * Use the command chmod to set permission.
	 */
	public void setPermission(Path p, FsPermission permission)
			throws IOException {
		if (NativeIO.isAvailable()) {
			NativeIO.chmod(new File(p.toUri().getPath()).getCanonicalPath(),
					permission.toShort());
		} else {
			execCommand(new File(p.toUri().getPath()), Shell.SET_PERMISSION_COMMAND,
					String.format("%05o", permission.toShort()));
		}
	}

	private static String execCommand(File f, String... cmd) throws IOException {
		String[] args = new String[cmd.length + 1];
		System.arraycopy(cmd, 0, args, 0, cmd.length);
		args[cmd.length] = FileUtil.makeShellPath(f, true);
		String output = Shell.execCommand(args);
		return output;
	}
}