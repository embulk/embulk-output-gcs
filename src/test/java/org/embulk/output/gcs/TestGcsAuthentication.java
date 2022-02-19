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

import com.google.common.base.Throwables;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.units.LocalFile;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

import java.nio.file.NoSuchFileException;
import java.security.InvalidKeyException;
import java.util.Base64;
import java.util.Optional;

public class TestGcsAuthentication
{
    private static Optional<String> GCP_EMAIL;
    private static Optional<String> GCP_P12_KEYFILE;
    private static Optional<String> GCP_JSON_KEYFILE;
    private static String GCP_BUCKET;
    private static final String GCP_APPLICATION_NAME = "embulk-output-gcs";

    /*
     * This test case requires environment variables
     *   GCP_EMAIL
     *   GCP_P12_KEYFILE
     *   GCP_JSON_KEYFILE
     *   GCP_BUCKET
     */
    @BeforeClass
    public static void initializeConstant()
    {
        GCP_EMAIL = Optional.of(System.getenv("GCP_EMAIL"));
        GCP_JSON_KEYFILE = Optional.of(System.getenv("GCP_JSON_KEYFILE"));
        GCP_P12_KEYFILE = Optional.of(System.getenv("GCP_PRIVATE_KEYFILE"));
        GCP_BUCKET = System.getenv("GCP_BUCKET");
        // skip test cases, if environment variables are not set.
        assumeNotNull(GCP_EMAIL, GCP_P12_KEYFILE, GCP_JSON_KEYFILE, GCP_BUCKET);
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testGetServiceAccountCredentialThrowFileNotFoundException()
    {
        Optional<String> notFoundP12Keyfile = Optional.of("/path/to/notfound.p12");
        ConfigSource configSource = config(AuthMethod.private_key);
        configSource.set("p12_keyfile", notFoundP12Keyfile);
        try {
            CONFIG_MAPPER.map(configSource, PluginTask.class);
            fail();
        }
        catch (Exception ex) {
            Assert.assertTrue(Throwables.getRootCause(ex) instanceof NoSuchFileException);
        }
    }

    @Test
    public void testGetGcsClientUsingServiceAccountCredentialSuccess() throws Exception
    {
        ConfigSource configSource = config(AuthMethod.private_key);
        byte[] keyBytes = Base64.getDecoder().decode(GCP_P12_KEYFILE.get());
        Optional<LocalFile> p12Key = Optional.of(LocalFile.ofContent(keyBytes));
        configSource.set("p12_keyfile", p12Key);
        PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
        GcsAuthentication auth = new GcsAuthentication(task);
        auth.getGcsClient();
    }

    @Test(expected = ConfigException.class)
    public void testGetGcsClientUsingServiceAccountCredentialThrowConfigException() throws Exception
    {
        ConfigSource configSource = config(AuthMethod.private_key);
        byte[] keyBytes = Base64.getDecoder().decode(GCP_P12_KEYFILE.get());
        Optional<LocalFile> p12Key = Optional.of(LocalFile.ofContent(keyBytes));
        configSource.set("p12_keyfile", p12Key);
        configSource.set("bucket", "non-exists-bucket");
        PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
        GcsAuthentication auth = new GcsAuthentication(task);
        auth.getGcsClient();
        fail();
    }

    @Test
    public void testGetServiceAccountCredentialFromJsonThrowFileFileNotFoundException()
    {
        Optional<String> notFoundJsonKeyfile = Optional.of("/path/to/notfound.json");
        ConfigSource configSource = config(AuthMethod.json_key);
        configSource.set("json_keyfile", notFoundJsonKeyfile);
        try {
            CONFIG_MAPPER.map(configSource, PluginTask.class);
            fail();
        }
        catch (Exception ex) {
            Assert.assertTrue(Throwables.getRootCause(ex) instanceof NoSuchFileException);
        }
    }

    @Test
    public void testGetServiceAccountCredentialFromInvalidJsonKey()
    {
        String jsonKey = "{\n" +
                "\"type\": \"service_account\",\n" +
                "\"project_id\": \"test\",\n" +
                "\"private_key_id\": \"private_key_id\",\n" +
                "\"private_key\": \"-----BEGIN PRIVATE KEY-----\\nInvalidKey\\n-----END PRIVATE KEY-----\\n\",\n" +
                "\"client_email\": \"test@test.iam.gserviceaccount.com\",\n" +
                "\"client_id\": \"433252345345\",\n" +
                "\"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
                " \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
                "\"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
                "\"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/test.iam.gserviceaccount.com\"\n" +
                "}";

        Optional<LocalFile> invalidJsonKeyfile = Optional.of(LocalFile.ofContent(jsonKey.getBytes()));
        ConfigSource configSource = config(AuthMethod.json_key);
        configSource.set("json_keyfile", invalidJsonKeyfile);
        try {
            PluginTask task =  CONFIG_MAPPER.map(configSource, PluginTask.class);
            GcsAuthentication auth = new GcsAuthentication(task);
            auth.getGcsClient();
            fail();
        }
        catch (Exception ex) {
            Assert.assertTrue(Throwables.getRootCause(ex) instanceof InvalidKeyException);
        }
    }

    @Test
    public void testGetServiceAccountCredentialFromJsonSuccess() throws Exception
    {
        ConfigSource configSource = config(AuthMethod.json_key);
        Optional<LocalFile> jsonKeyfile = Optional.of(LocalFile.ofContent(GCP_JSON_KEYFILE.get().getBytes()));
        configSource.set("json_keyfile", jsonKeyfile);
        PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
        GcsAuthentication auth = new GcsAuthentication(task);
        auth.getGcsClient();
    }

    @Test(expected = ConfigException.class)
    public void testGetServiceAccountCredentialFromJsonThrowConfigException() throws Exception
    {
        ConfigSource configSource = config(AuthMethod.json_key);
        Optional<LocalFile> jsonKeyfile = Optional.of(LocalFile.ofContent(GCP_JSON_KEYFILE.get().getBytes()));
        configSource.set("json_keyfile", jsonKeyfile);
        configSource.set("bucket", "non-exists-bucket");
        PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
        GcsAuthentication auth = new GcsAuthentication(task);
        auth.getGcsClient();
        fail();
    }

    public ConfigSource config(AuthMethod authMethod)
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "")
                .set("last_path", "")
                .set("file_ext", ".csv")
                .set("service_account_email", GCP_EMAIL)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("max_connection_retry", 3);

        if (authMethod == AuthMethod.private_key) {
            config.set("auth_method", "private_key");
        }
        else if (authMethod == AuthMethod.json_key) {
            config.set("auth_method", "json_key");
        }
        else {
            config.set("auth_method", "compute_engine");
        }
        return config;
    }
}
