package org.embulk.output;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.api.services.storage.model.Objects;
import com.google.common.base.Throwables;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class GcsOutputPlugin implements FileOutputPlugin
{
    private static final Logger logger = Exec.getLogger(GcsOutputPlugin.class);

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

        if (task.getDeleteInAdvance()) {
            deleteFiles(task);
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
        return new GcsTransactionalFileOutput(task, client, taskIndex);
    }

    private void deleteFiles(PluginTask task)
    {
        logger.info("Start delete files operation");
        Storage client = createClient(task);
        try {
            List<StorageObject> items = listObjectsWithRetry(client, task.getBucket(), task.getPathPrefix(), task.getMaxConnectionRetry());
            if (items.size() == 0) {
                logger.info("no files were found");
                return;
            }
            for (StorageObject item : items) {
                deleteObjectWithRetry(client, item, task.getMaxConnectionRetry());
                logger.info("delete file: {}/{}", item.getBucket(), item.getName());
            }
        }
        catch (IOException ex) {
            throw new ConfigException(ex);
        }
    }

    private List<StorageObject> listObjectsWithRetry(Storage client, String bucket, String prefix, int maxConnectionRetry) throws IOException
    {
        try {
            return retryExecutor()
                .withRetryLimit(maxConnectionRetry)
                .withInitialRetryWait(500)
                .withMaxRetryWait(30 * 1000)
                .runInterruptible(new Retryable<List<StorageObject>>() {
                @Override
                public List<StorageObject> call() throws IOException
                {
                    Storage.Objects.List listObjects = client.objects().list(bucket).setDelimiter("/").setPrefix(prefix);
                    List<StorageObject> items = new LinkedList<StorageObject>();
                    String token = null;
                    do {
                        Objects objects = listObjects.execute();
                        if (objects.getItems() == null) {
                            break;
                        }
                        items.addAll(objects.getItems());
                        token = objects.getNextPageToken();
                        listObjects.setPageToken(token);
                    } while (token != null);
                    return items;
                }

                @Override
                public boolean isRetryableException(Exception exception)
                {
                    return true;
                }

                @Override
                public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException
                {
                    String message = String.format("GCS list request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
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

    private Void deleteObjectWithRetry(Storage client, StorageObject item, int maxConnectionRetry) throws IOException
    {
        try {
            return retryExecutor()
                .withRetryLimit(maxConnectionRetry)
                .withInitialRetryWait(500)
                .withMaxRetryWait(30 * 1000)
                .runInterruptible(new Retryable<Void>() {
                @Override
                public Void call() throws IOException
                {
                    client.objects().delete(item.getBucket(), item.getName()).execute();
                    return null;
                }

                @Override
                public boolean isRetryableException(Exception exception)
                {
                    return true;
                }

                @Override
                public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException
                {
                    String message = String.format("GCS delete request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
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

    private GcsAuthentication newGcsAuth(PluginTask task)
    {
        try {
            return new GcsAuthentication(
                    task.getAuthMethod().getString(),
                    task.getServiceAccountEmail(),
                    task.getP12Keyfile().map(localFileToPathString()),
                    task.getJsonKeyfile().map(localFileToPathString()),
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
}
