package org.embulk.output;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import org.embulk.config.TaskReport;
import org.embulk.spi.Buffer;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.embulk.output.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;

public class GcsTransactionalFileOutput implements TransactionalFileOutput
{
    private static final Logger logger = LoggerFactory.getLogger(GcsTransactionalFileOutput.class);

    private final int taskIndex;
    private final Storage client;
    private final String bucket;
    private final String pathPrefix;
    private final String pathSuffix;
    private final String sequenceFormat;
    private final String contentType;
    private final List<String> storageObjects = new ArrayList<>();
    private BlobId blobId = null;
    private int fileIndex = 0;
    private WriteChannel writer = null;
    private long byteCount = 0;
    private long totalByte = 0;

    GcsTransactionalFileOutput(PluginTask task, Storage client, int taskIndex)
    {
        this.taskIndex = taskIndex;
        this.client = client;
        this.bucket = task.getBucket();
        this.pathPrefix = task.getPathPrefix();
        this.pathSuffix = task.getFileNameExtension();
        this.sequenceFormat = task.getSequenceFormat();
        this.contentType = task.getContentType();
    }

    public void nextFile()
    {
        closeCurrentWriter();
        try {
            String blobName = generateRemotePath(pathPrefix, sequenceFormat, taskIndex, fileIndex, pathSuffix);
            blobId = BlobId.of(bucket, blobName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build();
            writer = client.writer(blobInfo);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void add(Buffer buffer)
    {
        try {
            writer.write(ByteBuffer.wrap(buffer.array(), buffer.offset(), buffer.limit()));
            byteCount = byteCount + buffer.limit();
            //104857600 = 100MB
            if (byteCount >= 104857600) {
                totalByte = totalByte + byteCount;
                logger.info("Uploaded {} bytes", totalByte);
                byteCount = 0;
            }
        }
        catch (Exception ex) {
            //clean up file if exist
            try {
                boolean deleted = client.delete(blobId);
                logger.info("  Delete file: {} > deleted? {}", blobId.getName(), deleted);
            }
            catch (Exception e) {
                logger.warn("Failed to delete file: {}, error message: {}", blobId.getName(), e.getMessage());
            }
            throw new RuntimeException(ex);
        }
        finally {
            buffer.release();
        }
    }

    @Override
    public void finish()
    {
        logger.info("Uploaded total {} bytes.", totalByte + byteCount);
        closeCurrentWriter();
        //query blob again to check
        Blob blob = client.get(blobId);
        logger.info("Upload {} successfully.", blobId.getName());
        storageObjects.add(blob.getBlobId().toString());
    }

    @Override
    public void close()
    {
        closeCurrentWriter();
    }

    @Override
    public void abort()
    {
    }

    @Override
    public TaskReport commit()
    {
        TaskReport report = CONFIG_MAPPER_FACTORY.newTaskReport();
        report.set("files", storageObjects);
        return report;
    }

    /**
     * GCS has character limitation in object names.
     * @see https://cloud.google.com/storage/docs/naming#objectnames
     * Although "." isn't listed at above pages, we can't access "./" path from GUI console.
     * And in many cases, user don't intend of creating "/" directory under the bucket.
     * This method normalizes path when it contains "./" and "/" and its variations at the beginning
     */
    @VisibleForTesting
    public static String generateRemotePath(String pathPrefix, String sequenceFormat, int taskIndex, int fileIndex, String pathSuffix)
    {
        String path = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + pathSuffix;
        return path.replaceFirst("^\\.*/*", "");
    }

    private void closeCurrentWriter()
    {
        if (writer != null && writer.isOpen()) {
            try {
                writer.close();
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
