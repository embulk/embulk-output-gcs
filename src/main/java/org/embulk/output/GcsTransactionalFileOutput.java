package org.embulk.output;

import com.google.api.client.http.InputStreamContent;
import com.google.api.client.repackaged.org.apache.commons.codec.binary.Base64;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Throwables;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class GcsTransactionalFileOutput implements TransactionalFileOutput
{
    private static final Logger logger = Exec.getLogger(GcsTransactionalFileOutput.class);

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

    GcsTransactionalFileOutput(PluginTask task, Storage client, int taskIndex)
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

    /**
     * GCS has character limitation in object names.
     * @see https://cloud.google.com/storage/docs/naming#objectnames
     * Although "." isn't listed at above pages, we can't access "./" path from GUI console.
     * And in many cases, user don't intend of creating "/" directory under the bucket.
     * This method normalizes path when it contains "./" and "/" and its variations at the beginning
     */
    private static String generateRemotePath(String pathPrefix, String sequenceFormat, int taskIndex, int fileIndex, String pathSuffix)
    {
        String path = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + pathSuffix;
        return path.replaceFirst("^\\.*/*", "");
    }
}
