package com.ibm.whia.data_loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.opencsv.CSVReader;

public class DataLoader {
	private static Logger logger = LoggerFactory.getLogger(DataLoader.class);
	private static ConcurrentMap<String, MetricsData> metrics = new ConcurrentHashMap<String, MetricsData>();
	private static boolean useSync = true;
	private static boolean useLoop = false;
	private static int delay = 0;

	public static void main(String[] args) {
		if (args == null || args.length < 3) {
			logger.info("Usage: java -jar data_loader.jar cassandra_host cassandra_key_space input_file_or_directory");
			System.exit(0);
		}
		String hostName = args[0]; // localhost
		String keySpace = args[1]; // whia_
		String inputFileName = args[2]; // data/alpr-1300 OR
										// data/PersonModel.csv
		if (args.length > 3) {
			if (args[3].equalsIgnoreCase("as")) {
				useSync = false;
				logger.info("Using asynchronous loading");
			} else {
				logger.info("Using synchronous loading");
			}
			try {
				delay = Integer.parseInt(args[3]);
				useLoop = true;
			} catch (NumberFormatException ex) {
				logger.info("Incorrect parameter value for the delay: " + args[3]);
				System.exit(-1);
			}
		}
		logger.info("Insert delay is " + delay + " ms" + ((useLoop) ? ", used in loop." : ""));

		Timer timer = new Timer();
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				logger.debug("Metrix: " + metrics.toString());
				logger.info(String.format("%32s%10s%10s%10s", "TableName", "Ins/Upd", "ms", "tr/sec"));
				for (Map.Entry<String, MetricsData> entry : metrics.entrySet()) {
					logger.info(String.format("%32s%10d%10d%10.2f", entry.getKey(), entry.getValue().getTransactions(),
							entry.getValue().getTime(), entry.getValue().getRate()));
				}
			}
		};
		timer.schedule(task, 60 * 1000, 60 * 1000);

		File inputFile = new File(inputFileName);
		String[] extensions = new String[] { "csv" };
		List<File> files = null;
		Cluster cluster = Cluster.builder().addContactPoint(hostName).build();
		boolean isCqlScript = false;
		if (inputFile.isDirectory()) {
			files = (List<File>) FileUtils.listFiles(inputFile, extensions, true);
		} else {
			if (FilenameUtils.getExtension(inputFileName).equalsIgnoreCase("cql")) {
				Session session = cluster.connect(keySpace);
				try {
					isCqlScript = true;
					String statementAll = new String(Files.readAllBytes(Paths.get(inputFileName)));
					String[] statements = statementAll.split(";");
					for (String statement : statements) {
						statement = statement.trim();
						if (!StringUtils.isEmpty(statement) && !isCqlComment(statement)) {
							logger.info("Executing statement: " + statement);
							session.execute(statement);
						}
					}
				} catch (Exception ex) {
					logger.error(ex.toString(), ex);
				} finally {
					session.close();
				}
			} else {
				files = new ArrayList<File>();
				files.add(inputFile);
			}
		}
		if (!isCqlScript) {
			if (useLoop) {
				while (true) {
					processCsvFiles(cluster, keySpace, files);
				}
			} else {
				processCsvFiles(cluster, keySpace, files);
			}
		}
		cluster.close();
	}

	public static void processCsvFiles(final Cluster cluster, final String keySpace, final List<File> files) {
		for (File file : files) {
			Session session = cluster.connect(keySpace);
			try {
				processCsvFile(session, keySpace, file);
			} catch (IOException ex) {
				logger.error(ex.toString(), ex);
			} finally {
				session.close();
			}
		}
	}

	private static void processCsvFile(final Session session, final String keySpace, final File file)
			throws FileNotFoundException, IOException {
		logger.info("Processing file: " + file.getPath());
		String tableName = "\"" + FilenameUtils.getBaseName(file.getPath()) + "\"";
		TableMetadata tableMetadata = fetchTableMetadata(session, keySpace, tableName);
		CSVReader csvReader = new CSVReader(new BufferedReader(new FileReader(file)));
		Map<String, DataType> columnMetadata = fetchColumnMetadata(tableMetadata);
		if (columnMetadata != null) {
			String[] line = null;
			boolean isFirstline = true;
			String[] columns = null;
			String columnsString = null;
			int totalLineNumber = 0;
			int processedLineNumber = 0;
			long startTime = System.currentTimeMillis();
			while ((line = csvReader.readNext()) != null) {
				totalLineNumber++;
				logger.debug("Line: " + Arrays.toString(line));
				if (isFirstline) {
					columns = line;
					columnsString = "\"" + String.join("\",\"", line) + "\"";
					isFirstline = false;
				} else {
					String values = "";
					for (int i = 0; i < line.length; i++) {
						String columnName = columns[i];
						DataType columnType = columnMetadata.get(columnName);
						if (columnType != null) {
							if (columnType.getName() == Name.TEXT) {
								values += "'" + line[i].replaceAll("'", "''") + "'" + ",";
							} else if (columnType.getName() == Name.DATE) {
								if (!StringUtils.isEmpty(line[i])) {
									values += "'" + line[i] + "'" + ",";
								} else {
									values += "NULL" + ",";
								}
							} else if (columnType.getName() == Name.INT || columnType.getName() == Name.BIGINT) {
								if (!StringUtils.isEmpty(line[i])) {
									values += line[i] + ",";
								} else {
									values += "0" + ",";
								}
							} else if (columnType.getName() == Name.DOUBLE) {
								if (!StringUtils.isEmpty(line[i])) {
									values += line[i] + ",";
								} else {
									values += "0.0" + ",";
								}
							} else if (columnType.getName() == Name.BOOLEAN) {
								if (!StringUtils.isEmpty(line[i])) {
									values += line[i] + ",";
								} else {
									values += "false" + ",";
								}
							} else if (columnType.getName() == Name.UUID) {
								if (!StringUtils.isEmpty(line[i])) {
									values += line[i] + ",";
								} else {
									values += (new UUID(0, 0)).toString() + ",";
								}
							} else {
								values += line[i] + ",";
							}
						} else {
							logger.error("Column is not found: " + columnName);
						}
					}
					if (values.length() > 0)
						values = values.substring(0, values.length() - 1);
					String statement = "INSERT INTO " + tableName + " (" + columnsString + ") VALUES (" + values + ")";
					logger.debug("Statement: " + statement);
					try {
						if (useSync) {
							session.execute(statement);
						} else {
							session.executeAsync(statement);
						}
						processedLineNumber++;
						Thread.sleep(delay);
					} catch (Exception ex) {
						logger.error("Statement: " + statement);
						logger.error(ex.toString(), ex);
					}
				}
				if (totalLineNumber % 100 == 0) {
					logger.debug("Processing " + totalLineNumber + " row in the file: " + file.getPath());
				}
			}
			long stopTime = System.currentTimeMillis();
			long elapsedTime = Math.max(stopTime - startTime - processedLineNumber * delay, 0);
			MetricsData entry = metrics.get(tableName);
			if (entry == null) {
				double rate = 1000. * totalLineNumber / elapsedTime;
				entry = new MetricsData(totalLineNumber, elapsedTime, rate);
			} else {
				double rate = 1000. * (totalLineNumber + entry.getTransactions()) / (elapsedTime + entry.getTime());
				entry = new MetricsData(totalLineNumber + entry.getTransactions(), elapsedTime + entry.getTime(), rate);
			}
			metrics.put(tableName, entry);
			logger.info("Processed " + (processedLineNumber + 1) + " rows from " + totalLineNumber
					+ " rows in the file: " + file.getPath() + " in " + elapsedTime + " ms ("
					+ String.format("%.2f", 1000. * totalLineNumber / elapsedTime) + " transactions per sec)");
		} else {
			logger.error("Table is not found in the database: " + tableName);
		}
		csvReader.close();
	}

	private static TableMetadata fetchTableMetadata(final Session session, final String keySpace,
			final String tableName) {
		Metadata metadata = session.getCluster().getMetadata();
		KeyspaceMetadata ksMetadata = metadata.getKeyspace(keySpace);
		if (ksMetadata != null) {
			return ksMetadata.getTable(tableName);
		} else {
			return null;
		}
	}

	private static Map<String, DataType> fetchColumnMetadata(final TableMetadata tableMetadata) {
		Map<String, DataType> columns = null;
		if (tableMetadata != null) {
			columns = new HashMap<String, DataType>();
			for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
				columns.put(columnMetadata.getName(), columnMetadata.getType());
			}
		}
		return columns;
	}

	private static boolean isCqlComment(final String statement) {
		return statement.startsWith("--") || statement.startsWith("//");
	}
}

class MetricsData {
	public int transactions;
	public long time;
	public double rate;

	public MetricsData(final int transactions, final long time, final double rate) {
		super();
		this.transactions = transactions;
		this.time = time;
		this.rate = rate;
	}

	public int getTransactions() {
		return transactions;
	}

	public long getTime() {
		return time;
	}

	public double getRate() {
		return rate;
	}
}
