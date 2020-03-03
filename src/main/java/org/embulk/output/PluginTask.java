package org.embulk.output;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.spi.unit.LocalFile;

import java.util.Optional;

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

    @Config("delete_in_advance")
    @ConfigDefault("false")
    boolean getDeleteInAdvance();
}
