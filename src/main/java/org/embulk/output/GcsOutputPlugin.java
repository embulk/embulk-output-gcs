package org.embulk.output;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GcsOutputPlugin implements FileOutputPlugin {
	private static final Logger logger = Exec.getLogger(GcsOutputPlugin.class);
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	public interface PluginTask extends Task {
		@Config("bucket")
		public String getBucket();

		@Config("path_prefix")
		public String getPathPrefix();

		@Config("file_ext")
		public String getFileNameExtension();

		@Config("sequence_format")
		@ConfigDefault("\".%03d.%02d\"")
		public String getSequenceFormat();

		@Config("content_type")
		@ConfigDefault("\"application/octet-stream\"")
		public String getContentType();

		@Config("service_account_email")
		public String getServiceAccountEmail();

		@Config("p12_keyfile_path")
		public String getP12KeyfilePath();

		@Config("application_name")
		@ConfigDefault("\"embulk-output-gcs\"")
		public String getApplicationName();
	}

	@Override
	public ConfigDiff transaction(ConfigSource config,
	                              int taskCount,
	                              FileOutputPlugin.Control control) {
		PluginTask task = config.loadConfig(PluginTask.class);
		return resume(task.dump(), taskCount, control);
	}

	@Override
	public ConfigDiff resume(TaskSource taskSource,
	                         int taskCount,
	                         FileOutputPlugin.Control control) {
		control.run(taskSource);
		return Exec.newConfigDiff();
	}

	@Override
	public void cleanup(TaskSource taskSource,
	                    int taskCount,
	                    List<CommitReport> successCommitReports) {
	}

	@Override
	public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex) {
		PluginTask task = taskSource.loadTask(PluginTask.class);

		Storage client = createClient(task);
		return new TransactionalGcsFileOutput(task, client, taskIndex);
	}

	private GoogleCredential createCredential(final PluginTask task, final HttpTransport httpTransport) {
		try {
			// @see https://developers.google.com/accounts/docs/OAuth2ServiceAccount#authorizingrequests
			// @see https://cloud.google.com/compute/docs/api/how-tos/authorization
			// @see https://developers.google.com/resources/api-libraries/documentation/storage/v1/java/latest/com/google/api/services/storage/STORAGE_SCOPE.html
			GoogleCredential cred = new GoogleCredential.Builder()
					.setTransport(httpTransport)
					.setJsonFactory(JSON_FACTORY)
					.setServiceAccountId(task.getServiceAccountEmail())
					.setServiceAccountScopes(ImmutableList.of(StorageScopes.DEVSTORAGE_READ_WRITE))
					.setServiceAccountPrivateKeyFromP12File(new File(task.getP12KeyfilePath()))
					.build();
			return cred;
		} catch (IOException ex) {
			logger.error(String.format("Could not load client secrets file %s", task.getP12KeyfilePath()));
			throw Throwables.propagate(ex);
		} catch (GeneralSecurityException ex) {
			logger.error("Google Authentication was failed");
			throw Throwables.propagate(ex);
		}
	}

	private Storage createClient(final PluginTask task) {
		HttpTransport httpTransport = new ApacheHttpTransport.Builder().build();
		Credential credential = createCredential(task, httpTransport);
		Storage client = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
				.setApplicationName(task.getApplicationName())
				.build();
		return client;
	}

	static class TransactionalGcsFileOutput implements TransactionalFileOutput {
		private final int taskIndex;
		private final Storage client;
		private final String bucket;
		private final String pathPrefix;
		private final String pathSuffix;
		private final String sequenceFormat;
		private final String contentType;
		private final List<StorageObject> storageObjects = new ArrayList<>();

		private int fileIndex = 0;
		private int callCount = 0;
		private PipedOutputStream currentStream = null;
		private Future<StorageObject> currentUpload = null;

		TransactionalGcsFileOutput(PluginTask task, Storage client, int taskIndex) {
			this.taskIndex = taskIndex;
			this.client = client;
			this.bucket = task.getBucket();
			this.pathPrefix = task.getPathPrefix();
			this.pathSuffix = task.getFileNameExtension();
			this.sequenceFormat = task.getSequenceFormat();
			this.contentType = task.getContentType();
		}

		public void nextFile() {
			closeCurrentUpload();
			currentStream = new PipedOutputStream();
			String path = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + pathSuffix;
			logger.info("Uploading '{}/{}'", bucket, path);
			currentUpload = startUpload(path, contentType, currentStream);
			fileIndex++;
		}

		@Override
		public void add(Buffer buffer) {
			try {
				logger.debug("#add called {} times for taskIndex {}", callCount, taskIndex);
				currentStream.write(buffer.array(), buffer.offset(), buffer.limit());
				callCount++;
			} catch (IOException ex) {
				throw new RuntimeException(ex);
			} finally {
				buffer.release();
			}
		}

		@Override
		public void finish() {
			closeCurrentUpload();
		}

		@Override
		public void close() {
			closeCurrentUpload();
		}

		@Override
		public void abort() {
		}

		@Override
		public CommitReport commit() {
			CommitReport report = Exec.newCommitReport();
			report.set("files", storageObjects);
			return report;
		}

		private void closeCurrentUpload() {
			try {
				if (currentStream != null) {
					currentStream.close();
					currentStream = null;
				}

				if (currentUpload != null) {
					StorageObject obj = currentUpload.get();
					storageObjects.add(obj);
					logger.info("Uploaded '{}/{}' to {}bytes", obj.getBucket(), obj.getName(), obj.getSize());
					currentUpload = null;
				}

				callCount = 0;
			} catch (InterruptedException | ExecutionException | IOException ex) {
				throw Throwables.propagate(ex);
			}
		}

		private Future<StorageObject> startUpload(String path, String contentType, PipedOutputStream output) {
			try {
				final ExecutorService executor = Executors.newCachedThreadPool();

				PipedInputStream inputStream = new PipedInputStream(output);
				InputStreamContent mediaContent = new InputStreamContent(contentType, inputStream);
				mediaContent.setCloseInputStream(true);

				StorageObject objectMetadata = new StorageObject();
				objectMetadata.setName(path);

				final Storage.Objects.Insert insert = client.objects().insert(bucket, objectMetadata, mediaContent);
				insert.setDisableGZipContent(true);
				return executor.submit(new Callable<StorageObject>() {
					@Override
					public StorageObject call() throws InterruptedException {
						try {
							return insert.execute();
						} catch (IOException ex) {
							throw Throwables.propagate(ex);
						} finally {
							executor.shutdown();
						}
					}
				});
			} catch (IOException ex) {
				throw Throwables.propagate(ex);
			}
		}
	}
}
