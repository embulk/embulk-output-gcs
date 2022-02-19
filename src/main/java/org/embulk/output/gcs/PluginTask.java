/*
 * Copyright 2018 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.gcs;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.embulk.util.config.units.LocalFile;

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

    @Config("initial_retry_interval_millis")
    @ConfigDefault("500")
    int getInitialRetryIntervalMillis();

    @Config("maximum_retry_interval_millis")
    @ConfigDefault("30000")
    int getMaximumRetryIntervalMillis();

    @Config("store_pass")
    @ConfigDefault("\"notasecret\"")
    String getStorePass();

    @Config("key_pass")
    @ConfigDefault("\"notasecret\"")
    String getKeyPass();
}
