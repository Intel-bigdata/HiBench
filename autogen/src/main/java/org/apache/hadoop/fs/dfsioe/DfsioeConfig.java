package org.apache.hadoop.fs.dfsioe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public final class DfsioeConfig {
	private static DfsioeConfig instance = null;

	private DfsioeConfig() {
	}

	public static synchronized DfsioeConfig getInstance() {
		if (instance == null)
			instance = new DfsioeConfig();
		return instance;
	}

	private static final Log LOG = LogFactory.getLog(DfsioeConfig.class);

	private String testRootDir;
	private Path controlDir;
	private Path writeDir;
	private Path readDir;
	private Path dataDir;
	private Path reportDir;
	private Path reportTmp;

	public String getTestRootDir() {
		if (testRootDir == null) {
			throw new RuntimeException(
					"testRootDir should not be null at this point");
		}
		return testRootDir;
	}

	public String getTestRootDir(Configuration conf) {
		String propName = "test.build.data";
		if (testRootDir == null) {
			testRootDir = System.getProperty(propName);
			if( testRootDir != null ) {
				LOG.info("Found '" + propName + "' in System properties, value is '" + testRootDir + "'");
			} else {
				testRootDir = safeGet(conf, propName);
				LOG.info("Found '" + propName + "' in Hadoop configuration, value is '" + testRootDir + "'");
			}
			LOG.info("Setting testRootDir to '" + testRootDir + "'");
		}

		return testRootDir;
	}

	public Path getControlDir() {
		if (controlDir == null) {
			controlDir = new Path(getTestRootDir(), "io_control");
		}
		return controlDir;
	}

	public Path getControlDir(Configuration conf) {
		controlDir = new Path(getTestRootDir(conf), "io_control");

		return controlDir;
	}

	public Path getWriteDir() {
		if (writeDir == null) {
			writeDir = new Path(getTestRootDir(), "io_write");
		}
		return writeDir;
	}

	public Path getWriteDir(Configuration conf) {
		writeDir = new Path(getTestRootDir(conf), "io_write");

		return writeDir;
	}

	public Path getReadDir() {
		if (readDir == null) {
			readDir = new Path(getTestRootDir(), "io_read");
		}
		return readDir;
	}

	public Path getReadDir(Configuration conf) {
		readDir = new Path(getTestRootDir(conf), "io_read");

		return readDir;
	}

	public Path getDataDir() {
		if (dataDir == null) {
			dataDir = new Path(getTestRootDir(), "io_data");
		}
		return dataDir;
	}

	public Path getDataDir(Configuration conf) {
		dataDir = new Path(getTestRootDir(conf), "io_data");

		return dataDir;
	}

	public Path getReportDir() {
		if (reportDir == null) {
			reportDir = new Path(getTestRootDir(), "reports");
		}
		return reportDir;
	}

	public Path getReportDir(Configuration conf) {
		reportDir = new Path(getTestRootDir(conf), "reports");

		return reportDir;
	}

	public Path getReportTmp() {
		if (reportTmp == null) {
			reportTmp = new Path(getTestRootDir(), "_merged_reports.txt");
		}
		return reportTmp;
	}

	public Path getReportTmp(Configuration conf) {
		reportTmp = new Path(getTestRootDir(conf), "_merged_reports.txt");

		return reportTmp;
	}

	public String safeGet(Configuration conf, String prop) {
		if (conf == null) {
			throw new RuntimeException("null config not allowed");
		}
		if (conf.get(prop) == null) {
			throw new RuntimeException("Property " + prop + " cannot be empty");
		}
		return conf.get(prop);
	}

}