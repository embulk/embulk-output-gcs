package org.embulk.output;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;

public class GcsOutputPlugin implements FileOutputPlugin
{
    private static final Logger logger = Exec.getLogger(GcsOutputPlugin.class);
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    public interface PluginTask extends Task
    {
        @Config("bucket")
        public String getBucket();

        @Config("path_prefix")
        @ConfigDefault("\"\"")
        public String getPathPrefix();

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
            FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Storage client = createClient(task);
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileOutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("gcs output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount,
            List<CommitReport> successCommitReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        Storage client = createClient(task);
        return new GcsFileOutput(task, client, taskIndex);
    }

    /**
     * @see https://developers.google.com/accounts/docs/OAuth2ServiceAccount#authorizingrequests
     */
    private GoogleCredential createCredential(final PluginTask task, final HttpTransport httpTransport)
    {
        GoogleCredential cred = null;
        try {
            // @see https://cloud.google.com/compute/docs/api/how-tos/authorization
            // @see https://developers.google.com/resources/api-libraries/documentation/storage/v1/java/latest/com/google/api/services/storage/STORAGE_SCOPE.html
            cred = new GoogleCredential.Builder()
                    .setTransport(httpTransport)
                    .setJsonFactory(JSON_FACTORY)
                    .setServiceAccountId(task.getServiceAccountEmail())
                    .setServiceAccountScopes(ImmutableList.of(StorageScopes.DEVSTORAGE_READ_WRITE))
                    .setServiceAccountPrivateKeyFromP12File(new File(task.getP12KeyfilePath()))
                    .build();
        } catch (IOException e) {
            logger.warn(String.format("Could not load client secrets file %s", task.getP12KeyfilePath()));
        } catch (GeneralSecurityException e) {
            logger.warn ("Google Authentication was failed");
        } finally {
            return cred;
        }
    }

    private Storage createClient(final PluginTask task)
    {
        try {
            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            Credential credential = createCredential(task, httpTransport);
            Storage client = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(task.getApplicationName())
                .build();
            return client;
        } catch (GeneralSecurityException|IOException e) {
            throw Throwables.propagate(e);
        }
    }

    static class GcsFileOutput implements TransactionalFileOutput
    {
        private final Storage client;
        private final String bucket;
        private final String pathPrefix;
        private final int taskIndex;
        private int seqId;
        private int callCount;
        private PipedOutputStream currentStream;
        private Future<Void> currentUpload;

        GcsFileOutput(PluginTask task, Storage client, int taskIndex)
        {
            this.client = client;
            this.taskIndex = taskIndex;
            this.bucket = task.getBucket();
            this.pathPrefix = task.getPathPrefix();
            this.seqId = 0;
            this.currentStream = null;
        }

        public void nextFile()
        {
            closeCurrentUpload();
            currentStream = new PipedOutputStream();
            currentUpload = startUpload(currentStream, taskIndex, seqId);
            seqId++;
        }

        @Override
        public void add(Buffer buffer)
        {
            try {
                logger.info("taskIndex: {}, count: {}", taskIndex, callCount);
                currentStream.write(buffer.array(), buffer.offset(), buffer.limit());
                callCount++;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                buffer.release();
            }
        }

        @Override
        public void finish()
        {
        }

        @Override
        public void close()
        {
            closeCurrentUpload();
        }

        @Override
        public void abort()
        {
        }

        @Override
        public CommitReport commit()
        {
            return Exec.newCommitReport();
        }

        private void closeCurrentUpload() {
            try {
                if (currentStream != null) {
                    currentStream.close();
                    currentStream = null;
                }

                if (currentUpload != null) {
                    currentUpload.get();
                    currentUpload = null;
                }

                callCount = 0;
            } catch (InterruptedException | ExecutionException | IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private Future<Void> startUpload(PipedOutputStream output, int taskIndex, int seqId) {
            try {
                PipedInputStream inputStream = new PipedInputStream(output);
                InputStreamContent mediaContent = new InputStreamContent("application/octet-stream", inputStream);
                StorageObject objectMetadata = new StorageObject();
                String filename = String.format("%s%s%d_%d.csv", pathPrefix, "test", taskIndex, seqId);
                objectMetadata.setName(filename);
                logger.info(filename);

                final Storage.Objects.Insert insert = client.objects().insert(bucket, objectMetadata, mediaContent);
                return executor.submit(new Callable<Void>() {
                    @Override public Void call() throws InterruptedException {
                        try {
                            logger.info("upload start");
                            insert.execute();
                            return null;
                        } catch (IOException ex) {
                            throw Throwables.propagate(ex);
                        }
                    }
                });
            } catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }
    }
}
