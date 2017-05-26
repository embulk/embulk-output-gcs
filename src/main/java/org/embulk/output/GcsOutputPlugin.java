package org.embulk.output;

import com.google.api.client.http.InputStreamContent;
import com.google.api.client.repackaged.org.apache.commons.codec.binary.Base64;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GcsOutputPlugin implements FileOutputPlugin
{
    private static final Logger logger = Exec.getLogger(GcsOutputPlugin.class);

    public interface PluginTask extends Task
    {
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

        @Config("max_connection_retry")
        @ConfigDefault("10") // 10 times retry to connect GCS server if failed.
        int getMaxConnectionRetry();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  int taskCount,
                                  FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        if (task.getP12KeyfilePath().isPresent()) {
            if (task.getP12Keyfile().isPresent()) {
                throw new ConfigException("Setting both p12_keyfile_path and p12_keyfile is invalid");
            }
            try {
                task.setP12Keyfile(Optional.of(LocalFile.of(task.getP12KeyfilePath().get())));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        if (task.getAuthMethod().getString().equals("json_key")) {
            if (!task.getJsonKeyfile().isPresent()) {
                throw new ConfigException("If auth_method is json_key, you have to set json_keyfile");
            }
        }
        else if (task.getAuthMethod().getString().equals("private_key")) {
            if (!task.getP12Keyfile().isPresent() || !task.getServiceAccountEmail().isPresent()) {
                throw new ConfigException("If auth_method is private_key, you have to set both service_account_email and p12_keyfile");
            }
        }

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileOutputPlugin.Control control)
    {
        control.run(taskSource);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        Storage client = createClient(task);
        return new TransactionalGcsFileOutput(task, client, taskIndex);
    }

    private GcsAuthentication newGcsAuth(PluginTask task)
    {
        try {
            return new GcsAuthentication(
                    task.getAuthMethod().getString(),
                    task.getServiceAccountEmail(),
                    task.getP12Keyfile().transform(localFileToPathString()),
                    task.getJsonKeyfile().transform(localFileToPathString()),
                    task.getApplicationName()
            );
        }
        catch (GeneralSecurityException | IOException ex) {
            throw new ConfigException(ex);
        }
    }

    private Storage createClient(final PluginTask task)
    {
        try {
            GcsAuthentication auth = newGcsAuth(task);
            return auth.getGcsClient(task.getBucket(), task.getMaxConnectionRetry());
        }
        catch (ConfigException | IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private Function<LocalFile, String> localFileToPathString()
    {
        return new Function<LocalFile, String>()
        {
            public String apply(LocalFile file)
            {
                return file.getPath().toString();
            }
        };
    }

    static class TransactionalGcsFileOutput implements TransactionalFileOutput
    {
        private final int taskIndex;
        private final Storage client;
        private final String bucket;
        private final String pathPrefix;
        private final String pathSuffix;
        private final String sequenceFormat;
        private final String contentType;
        private final int maxConnectionRetry;
        private final List<StorageObject> storageObjects = new ArrayList<>();

        private int fileIndex = 0;
        private int callCount = 0;
        private BufferedOutputStream currentStream = null;
        private Future<StorageObject> currentUpload = null;
        private File tempFile = null;

        TransactionalGcsFileOutput(PluginTask task, Storage client, int taskIndex)
        {
            this.taskIndex = taskIndex;
            this.client = client;
            this.bucket = task.getBucket();
            this.pathPrefix = task.getPathPrefix();
            this.pathSuffix = task.getFileNameExtension();
            this.sequenceFormat = task.getSequenceFormat();
            this.contentType = task.getContentType();
            this.maxConnectionRetry = task.getMaxConnectionRetry();
        }

        public void nextFile()
        {
            closeCurrentUpload();
            try {
                tempFile = Exec.getTempFileSpace().createTempFile();
                currentStream = new BufferedOutputStream(new FileOutputStream(tempFile));
                fileIndex++;
            }
            catch (IOException ex) {
                Throwables.propagate(ex);
            }
        }

        @Override
        public void add(Buffer buffer)
        {
            try {
                logger.debug("#add called {} times for taskIndex {}", callCount, taskIndex);
                currentStream.write(buffer.array(), buffer.offset(), buffer.limit());
                callCount++;
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            finally {
                buffer.release();
            }
        }

        @Override
        public void finish()
        {
            String path = generateRemotePath(pathPrefix, sequenceFormat, taskIndex, fileIndex, pathSuffix);
            close();
            if (tempFile != null) {
                currentUpload = startUpload(path);
            }

            closeCurrentUpload();
        }

        @Override
        public void close()
        {
            try {
                if (currentStream != null) {
                    currentStream.close();
                    currentStream = null;
                }
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        @Override
        public void abort()
        {
        }

        @Override
        public TaskReport commit()
        {
            TaskReport report = Exec.newTaskReport();
            report.set("files", storageObjects);
            return report;
        }

        private void closeCurrentUpload()
        {
            try {
                if (currentUpload != null) {
                    StorageObject obj = currentUpload.get();
                    storageObjects.add(obj);
                    logger.info("Uploaded '{}/{}' to {}bytes", obj.getBucket(), obj.getName(), obj.getSize());
                    currentUpload = null;
                }

                callCount = 0;
            }
            catch (InterruptedException | ExecutionException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private Future<StorageObject> startUpload(final String path)
        {
            try {
                final ExecutorService executor = Executors.newCachedThreadPool();
                final String hash = getLocalMd5hash(tempFile.getAbsolutePath());

                return executor.submit(new Callable<StorageObject>() {
                    @Override
                    public StorageObject call() throws IOException
                    {
                        try {
                            logger.info("Uploading '{}/{}'", bucket, path);
                            return execUploadWithRetry(path, hash);
                        }
                        finally {
                            executor.shutdown();
                        }
                    }
                });
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private StorageObject execUploadWithRetry(final String path, final String localHash) throws IOException
        {
            try {
                return retryExecutor()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWait(500)
                    .withMaxRetryWait(30 * 1000)
                    .runInterruptible(new Retryable<StorageObject>() {
                    @Override
                    public StorageObject call() throws IOException, RetryGiveupException
                    {
                        try (final BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(tempFile))) {
                            InputStreamContent mediaContent = new InputStreamContent(contentType, inputStream);
                            mediaContent.setCloseInputStream(true);

                            StorageObject objectMetadata = new StorageObject();
                            objectMetadata.setName(path);

                            final Storage.Objects.Insert insert = client.objects().insert(bucket, objectMetadata, mediaContent);
                            insert.setDisableGZipContent(true);
                            StorageObject obj = insert.execute();

                            logger.info(String.format("Local Hash(MD5): %s / Remote Hash(MD5): %s", localHash, obj.getMd5Hash()));
                            return obj;
                        }
                    }

                    @Override
                    public boolean isRetryableException(Exception exception)
                    {
                        return true;
                    }

                    @Override
                    public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException
                    {
                        String message = String.format("GCS put request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
                                        retryCount, retryLimit, retryWait / 1000, exception.getClass(), exception.getMessage());
                        if (retryCount % 3 == 0) {
                            logger.warn(message, exception);
                        }
                        else {
                            logger.warn(message);
                        }
                    }

                    @Override
                    public void onGiveup(Exception firstException, Exception lastException) throws RetryGiveupException
                    {
                    }
                });
            }
            catch (RetryGiveupException ex) {
                throw Throwables.propagate(ex.getCause());
            }
            catch (InterruptedException ex) {
                throw new InterruptedIOException();
            }
        }

        /*
        MD5 hash sum on GCS bucket is encoded with base64.
        You can get same hash with following commands.
        $ openssl dgst -md5 -binary /path/to/file.txt | openssl enc -base64
        or
        $ gsutil hash -m /path/to/file.txt
         */
        private String getLocalMd5hash(String filePath) throws IOException
        {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File(filePath)))) {
                    byte[] buffer = new byte[256];
                    int len;
                    while ((len = input.read(buffer, 0, buffer.length)) >= 0) {
                        md.update(buffer, 0, len);
                    }
                    return new String(Base64.encodeBase64(md.digest()));
                }
            }
            catch (NoSuchAlgorithmException ex) {
                throw new ConfigException("MD5 algorism not found");
            }
        }
    }

    private static String generateRemotePath(String pathPrefix, String sequenceFormat, int taskIndex, int fileIndex, String pathSuffix)
    {
        String path = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + pathSuffix;
        return path.replaceFirst("\\.*/*", "");
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
