/*
 * Copyright 2015 The Embulk project
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

import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.TransportOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.embulk.config.ConfigException;
import org.embulk.util.config.units.LocalFile;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

public class GcsAuthentication
{
    private final Logger log = LoggerFactory.getLogger(GcsAuthentication.class);
    private final Optional<String> serviceAccountEmail;
    private final Optional<String> p12KeyFilePath;
    private final Optional<String> jsonKeyFilePath;
    private final String applicationName;
    private final HttpTransport httpTransport;
    private final JacksonFactory jsonFactory;
    private final GoogleCredentials credentials;
    private PluginTask task;

    public GcsAuthentication(PluginTask task) throws IOException, GeneralSecurityException
    {
        this.task = task;
        this.serviceAccountEmail = task.getServiceAccountEmail();
        this.p12KeyFilePath = task.getP12Keyfile().map(localFileToPathString());
        this.jsonKeyFilePath = task.getJsonKeyfile().map(localFileToPathString());
        this.applicationName = task.getApplicationName();
        this.httpTransport = new ApacheHttpTransport.Builder().build();
        this.jsonFactory = new JacksonFactory();

        if (task.getAuthMethod() == AuthMethod.compute_engine) {
            this.credentials = getComputeCredential();
        }
        else if (task.getAuthMethod() == AuthMethod.json_key) {
            this.credentials = getServiceAccountCredentialFromJsonFile();
        }
        else {
            this.credentials = getServiceAccountCredential();
        }
    }

    /**
     * @see https://developers.google.com/accounts/docs/OAuth2ServiceAccount#authorizingrequests
     */
    private GoogleCredentials getServiceAccountCredential() throws IOException, GeneralSecurityException
    {
        File p12 = new File(p12KeyFilePath.get());
        PrivateKey privateKey = SecurityUtils.loadPrivateKeyFromKeyStore(SecurityUtils.getPkcs12KeyStore(),
                new FileInputStream(p12), task.getStorePass(), "privatekey", task.getKeyPass());
        HttpTransportFactory transportFactory = () -> httpTransport;
        GoogleCredentials credentials = new ServiceAccountCredentials(null, serviceAccountEmail.get(),
            privateKey, null, Collections.singleton(StorageScopes.DEVSTORAGE_READ_WRITE), transportFactory, null);
        return credentials;
    }

    private GoogleCredentials getServiceAccountCredentialFromJsonFile() throws IOException
    {
        FileInputStream stream = new FileInputStream(jsonKeyFilePath.get());
        return GoogleCredentials.fromStream(stream)
                .createScoped(Collections.singleton(StorageScopes.DEVSTORAGE_READ_WRITE));
    }

    /**
     * @see http://developers.guge.io/accounts/docs/OAuth2ServiceAccount#creatinganaccount
     * @see https://developers.google.com/accounts/docs/OAuth2
     */
    private GoogleCredentials getComputeCredential() throws IOException
    {
        HttpTransportFactory transportFactory = () -> httpTransport;
        ComputeEngineCredentials credentials = new ComputeEngineCredentials(transportFactory);
        credentials.refreshAccessToken();
        return credentials;
    }

    public Storage getGcsClient() throws ConfigException, IOException
    {
        try {
            return RetryExecutor.builder()
                    .withRetryLimit(task.getMaxConnectionRetry())
                    .withInitialRetryWaitMillis(task.getInitialRetryIntervalMillis())
                    .withMaxRetryWaitMillis(task.getMaximumRetryIntervalMillis())
                    .build()
                    .runInterruptible(new Retryable<Storage>() {
                        @Override
                        public Storage call() throws IOException, RetryGiveupException
                        {
                            final TransportOptions transportOptions = HttpTransportOptions.newBuilder()
                                    .setConnectTimeout(30000) // in milliseconds
                                    .setReadTimeout(30000) // in milliseconds
                                    .build();

                            Storage client = StorageOptions.newBuilder()
                                    .setCredentials(credentials)
                                    .setTransportOptions(transportOptions)
                                    .build().getService();

                            // For throw ConfigException when authentication is fail.
                            client.list(task.getBucket(), Storage.BlobListOption.pageSize(1)).hasNextPage();
                            return client;
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            if (exception instanceof GoogleJsonResponseException || exception instanceof TokenResponseException || exception instanceof StorageException) {
                                int statusCode;
                                if (exception instanceof GoogleJsonResponseException) {
                                    if (((GoogleJsonResponseException) exception).getDetails() == null) {
                                        String content = "";
                                        if (((GoogleJsonResponseException) exception).getContent() != null) {
                                            content = ((GoogleJsonResponseException) exception).getContent();
                                        }
                                        log.warn("Invalid response was returned : {}", content);
                                        return true;
                                    }
                                    statusCode = ((GoogleJsonResponseException) exception).getDetails().getCode();
                                }
                                else if (exception instanceof TokenResponseException) {
                                    statusCode = ((TokenResponseException) exception).getStatusCode();
                                }
                                else {
                                    statusCode = ((StorageException) exception).getCode();
                                }

                                if (statusCode / 100 == 4) {
                                    return false;
                                }
                            }
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            String message = String.format("GCS GET request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getClass(), exception.getMessage());
                            if (retryCount % 3 == 0) {
                                log.warn(message, exception);
                            }
                            else {
                                log.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException)
                                throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof GoogleJsonResponseException || ex.getCause() instanceof TokenResponseException || ex.getCause() instanceof StorageException) {
                int statusCode = 0;
                if (ex.getCause() instanceof GoogleJsonResponseException) {
                    if (((GoogleJsonResponseException) ex.getCause()).getDetails() != null) {
                        statusCode = ((GoogleJsonResponseException) ex.getCause()).getDetails().getCode();
                    }
                }
                else if (ex.getCause() instanceof TokenResponseException) {
                    statusCode = ((TokenResponseException) ex.getCause()).getStatusCode();
                }
                else {
                    statusCode = ((StorageException) ex.getCause()).getCode();
                }
                if (statusCode / 100 == 4) {
                    throw new ConfigException(ex);
                }
            }
            throw new RuntimeException(ex);
        }
        catch (InterruptedException ex) {
            throw new InterruptedIOException();
        }
    }

    private Function<LocalFile, String> localFileToPathString()
    {
        return file -> file.getPath().toString();
    }
}
