package org.embulk.output;

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.unit.LocalFile;
import org.slf4j.Logger;

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

	public interface PluginTask extends Task {
		@Config("bucket")
		String getBucket();

		@Config("path_prefix")
		String getPathPrefix();

		@Config("file_ext")
		String getFileNameExtension();

		@Config("sequence_format")
		@ConfigDefault("\".%03d.%02d\"")
		String getSequenceFormat();

		@Config("content_type")
		@ConfigDefault("\"application/octet-stream\"")
		String getContentType();

		@Config("auth_method")
		@ConfigDefault("\"private_key\"")
		AuthMethod getAuthMethod();

		@Config("service_account_email")
		@ConfigDefault("null")
		Optional<String> getServiceAccountEmail();

		// kept for backward compatibility
		@Config("p12_keyfile_path")
		@ConfigDefault("null")
		Optional<String> getP12KeyfilePath();

		@Config("p12_keyfile")
		@ConfigDefault("null")
		Optional<LocalFile> getP12Keyfile();
		void setP12Keyfile(Optional<LocalFile> p12Keyfile);

		@Config("json_keyfile")
		@ConfigDefault("null")
		Optional<LocalFile> getJsonKeyfile();

		@Config("application_name")
		@ConfigDefault("\"embulk-output-gcs\"")
		String getApplicationName();
	}

	private static GcsAuthentication auth;

	@Override
	public ConfigDiff transaction(ConfigSource config,
	                              int taskCount,
	                              FileOutputPlugin.Control control) {
		PluginTask task = config.loadConfig(PluginTask.class);

		if (task.getP12KeyfilePath().isPresent()) {
			if (task.getP12Keyfile().isPresent()) {
				throw new ConfigException("Setting both p12_keyfile_path and p12_keyfile is invalid");
			}
			try {
				task.setP12Keyfile(Optional.of(LocalFile.of(task.getP12KeyfilePath().get())));
			} catch (IOException ex) {
				throw Throwables.propagate(ex);
			}
		}

		if (task.getAuthMethod().getString().equals("json_key")) {
			if (!task.getJsonKeyfile().isPresent()) {
				throw new ConfigException("If auth_method is json_key, you have to set json_keyfile");
			}
		} else if (task.getAuthMethod().getString().equals("private_key")) {
			if (!task.getP12Keyfile().isPresent() || !task.getServiceAccountEmail().isPresent()) {
				throw new ConfigException("If auth_method is private_key, you have to set both service_account_email and p12_keyfile");
			}
		}

		try {
			auth = new GcsAuthentication(
					task.getAuthMethod().getString(),
					task.getServiceAccountEmail(),
					task.getP12Keyfile().transform(localFileToPathString()),
					task.getJsonKeyfile().transform(localFileToPathString()),
					task.getApplicationName()
			);
		} catch (GeneralSecurityException | IOException ex) {
			throw new ConfigException(ex);
		}

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
	                    List<TaskReport> successTaskReports) {
	}

	@Override
	public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex) {
		PluginTask task = taskSource.loadTask(PluginTask.class);

		Storage client = createClient(task);
		return new TransactionalGcsFileOutput(task, client, taskIndex);
	}

	private Storage createClient(final PluginTask task) {
		Storage client = null;
		try {
			client = auth.getGcsClient(task.getBucket());
		} catch (IOException ex) {
			throw new ConfigException(ex);
		}

		return client;
	}

	private Function<LocalFile, String> localFileToPathString() {
		return new Function<LocalFile, String>()
		{
			public String apply(LocalFile file)
			{
				return file.getPath().toString();
			}
		};
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
		public TaskReport commit() {
			TaskReport report = Exec.newTaskReport();
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

	public enum AuthMethod
	{
		private_key("private_key"),
		compute_engine("compute_engine"),
		json_key("json_key");

		private final String string;

		AuthMethod(String string)
		{
			this.string = string;
		}

		public String getString()
		{
			return string;
		}
	}
}
