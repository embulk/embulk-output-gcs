/*
 * Copyright 2015 Kazuyuki Honda, and the Embulk project
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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.embulk.EmbulkSystemProperties;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.units.LocalFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class TestGcsOutputPlugin
{
    private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES;
    private static Optional<String> GCP_EMAIL;
    private static Optional<String> GCP_P12_KEYFILE;
    private static Optional<String> GCP_JSON_KEYFILE;
    private static String GCP_BUCKET;
    private static String GCP_BUCKET_DIRECTORY;
    private static String GCP_PATH_PREFIX;
    private static String LOCAL_PATH_PREFIX;
    private static String GCP_APPLICATION_NAME;

    static {
        final Properties properties = new Properties();
        properties.setProperty("default_guess_plugins", "gzip,bzip2,json,csv");
        EMBULK_SYSTEM_PROPERTIES = EmbulkSystemProperties.of(properties);
    }

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
        GCP_P12_KEYFILE = Optional.of(System.getenv("GCP_PRIVATE_KEYFILE"));
        GCP_JSON_KEYFILE = Optional.of(System.getenv("GCP_JSON_KEYFILE"));
        GCP_BUCKET = System.getenv("GCP_BUCKET");
        // skip test cases, if environment variables are not set.
        assumeNotNull(GCP_EMAIL, GCP_P12_KEYFILE, GCP_JSON_KEYFILE, GCP_BUCKET);

        GCP_BUCKET_DIRECTORY = System.getenv("GCP_BUCKET_DIRECTORY") != null ? getDirectory(System.getenv("GCP_BUCKET_DIRECTORY")) : getDirectory("");
        GCP_PATH_PREFIX = GCP_BUCKET_DIRECTORY + "output_";
        LOCAL_PATH_PREFIX = GcsOutputPlugin.class.getClassLoader().getResource("sample_01.csv").getPath();
        GCP_APPLICATION_NAME = "embulk-output-gcs";
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
            .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
            .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
            .registerPlugin(FileOutputPlugin.class, "gcs", GcsOutputPlugin.class)
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .build();

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private GcsOutputPlugin plugin;

    @Before
    public void createResources() throws GeneralSecurityException, NoSuchMethodException, IOException
    {
        plugin = new GcsOutputPlugin();
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertEquals("private_key", task.getAuthMethod().toString());
    }

    // p12_keyfile is null when auth_method is private_key
    @Test
    public void checkDefaultValuesP12keyNull() throws IOException
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", null)
                .set("formatter", formatterConfig());

        try {
            embulk.runOutput(config, Paths.get(LOCAL_PATH_PREFIX));
            fail("Expected Exception was not thrown.");
        }
        catch (PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            assertEquals("If auth_method is private_key, you have to set both service_account_email and p12_keyfile", ex.getCause().getMessage());
        }
    }

    // both p12_keyfile and p12_keyfile_path set
    @Test
    public void checkDefaultValuesConflictSetting() throws IOException
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("formatter", formatterConfig());

        config.set("p12_keyfile", Optional.of(LocalFile.ofContent("dummy".getBytes())));
        config.set("p12_keyfile_path", Optional.of("dummy_path"));
        try {
            embulk.runOutput(config, Paths.get(LOCAL_PATH_PREFIX));
            fail("Expected Exception was not thrown.");
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            assertEquals("Setting both p12_keyfile_path and p12_keyfile is invalid", ex.getCause().getMessage());
        }
    }

    // invalid p12keyfile when auth_method is private_key
    @Test
    public void checkDefaultValuesInvalidPrivateKey() throws IOException
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", "invalid-key.p12")
                .set("formatter", formatterConfig());
        try {
            embulk.runOutput(config, Paths.get(LOCAL_PATH_PREFIX));
            fail("Expected Exception was not thrown.");
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
        }
    }

    // json_keyfile is null when auth_method is json_key
    @Test
    public void checkDefaultValuesJsonKeyfileNull() throws IOException
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", null)
                .set("formatter", formatterConfig());

        try {
            embulk.runOutput(config, Paths.get(LOCAL_PATH_PREFIX));
            fail("Expected Exception was not thrown.");
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
        }
    }

    @Test
    public void testGcsClientCreateSuccessfully()
    {
        ConfigSource configSource = config();
        PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
        plugin.transaction(configSource, 1, new FileOutputControl()); // no errors happens
        plugin.createClient(task); // no errors happens
    }

    @Test
    public void testGcsClientCreateThrowConfigException()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", "non-exists-bucket")
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", Optional.of(LocalFile.ofContent(GCP_JSON_KEYFILE.get().getBytes())))
                .set("formatter", formatterConfig());

        plugin.transaction(config, 1, new FileOutputControl()); // no errors happens
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        try {
            plugin.createClient(task);
            fail("Expected Exception was not thrown.");
        }
        catch (Exception ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
        }
    }

    @Test
    public void testGcsFileOutputByOpen() throws Exception
    {
        ConfigSource configSource = config();
        PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
        Storage client = plugin.createClient(task);
        try {
            embulk.runOutput(configSource, Paths.get(LOCAL_PATH_PREFIX));
        }
        catch (Exception ex) {
            fail(ex.getMessage());
        }

        String remotePath = GCP_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();
        assertRecords(remotePath, client);
    }

    @Test
    public void testGenerateRemotePath() throws Exception
    {
        ConfigSource configSource = config();
        PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
        Storage storage = Mockito.mock(Storage.class);
        GcsTransactionalFileOutput fileOutput = new GcsTransactionalFileOutput(task, storage, 0);
        assertEquals("sample.000.01.csv", fileOutput.generateRemotePath("/sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", fileOutput.generateRemotePath("./sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", fileOutput.generateRemotePath("../sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", fileOutput.generateRemotePath("//sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("path/to/sample.000.01.csv", fileOutput.generateRemotePath("/path/to/sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("path/to/./sample.000.01.csv", fileOutput.generateRemotePath("path/to/./sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("path/to/../sample.000.01.csv", fileOutput.generateRemotePath("path/to/../sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", fileOutput.generateRemotePath("....../sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", fileOutput.generateRemotePath("......///sample", task.getSequenceFormat(), 0, 1, ".csv"));
    }

    public ConfigSource config()
    {
        byte[] keyBytes = Base64.getDecoder().decode(GCP_P12_KEYFILE.get());
        Optional<LocalFile> p12Key = Optional.of(LocalFile.ofContent(keyBytes));
        Optional<LocalFile> jsonKey = Optional.of(LocalFile.ofContent(GCP_JSON_KEYFILE.get().getBytes()));

        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", GCP_PATH_PREFIX)
                .set("last_path", "")
                .set("file_ext", ".csv")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", p12Key)
                .set("json_keyfile", jsonKey)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("formatter", formatterConfig());
    }

    private class FileOutputControl implements FileOutputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource)
        {
            return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
        }
    }

    private ImmutableMap<String, Object> formatterConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("header_line", "false");
        builder.put("timezone", "Asia/Tokyo");
        return builder.build();
    }

    private void assertRecords(String gcsPath, Storage client) throws Exception
    {
        ImmutableList<List<String>> records = getFileContentsFromGcs(gcsPath, client);
        assertEquals(4, records.size());
        {
            List<String> record = records.get(0);
            assertEquals("1", record.get(0));
            assertEquals("32864", record.get(1));
        }

        {
            List<String> record = records.get(1);
            assertEquals("2", record.get(0));
            assertEquals("14824", record.get(1));
        }

        {
            List<String> record = records.get(2);
            assertEquals("3", record.get(0));
            assertEquals("27559", record.get(1));
        }

        {
            List<String> record = records.get(3);
            assertEquals("4", record.get(0));
            assertEquals("11270", record.get(1));
        }
    }

    private ImmutableList<List<String>> getFileContentsFromGcs(String path, Storage client) throws Exception
    {
        ConfigSource config = config();
        Blob blob = client.get(BlobId.of(GCP_BUCKET, path));
        InputStream is = Channels.newInputStream(blob.reader());
        ImmutableList.Builder<List<String>> builder = new ImmutableList.Builder<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = reader.readLine()) != null) {
            List<String> records = Arrays.asList(line.split(",", 0));
            builder.add(records);
        }
        return builder.build();
    }

    private static String getDirectory(String dir)
    {
        if (dir != null && !dir.endsWith("/")) {
            dir = dir + "/";
        }
        if (dir.startsWith("/")) {
            dir = dir.replaceFirst("/", "");
        }
        return dir;
    }

    private byte[] convertInputStreamToByte(InputStream is) throws IOException
    {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        byte [] buffer = new byte[1024];
        while (true) {
            int len = is.read(buffer);
            if (len < 0) {
                break;
            }
            bo.write(buffer, 0, len);
        }
        return bo.toByteArray();
    }
}
