package org.embulk.output;

import com.google.api.services.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.test.TestingEmbulk;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Arrays;
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
    private FileOutputRunner runner;

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
        GCP_EMAIL = Optional.of(System.getenv("GCP_EMAIL") == null ? "test@example.com" : System.getenv("GCP_EMAIL"));
        GCP_P12_KEYFILE = Optional.of(System.getenv("GCP_P12_KEYFILE") == null ? "file" : System.getenv("GCP_P12_KEYFILE"));
        GCP_JSON_KEYFILE = Optional.of(System.getenv("GCP_JSON_KEYFILE") == null ? "key" : System.getenv("GCP_JSON_KEYFILE"));
        GCP_BUCKET = System.getenv("GCP_BUCKET") == null ? "bucket" : System.getenv("GCP_BUCKET");
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

    private GcsOutputPlugin plugin;

    /*
    @Before
    public void createResources() throws GeneralSecurityException, NoSuchMethodException, IOException
    {
        plugin = new GcsOutputPlugin();
        runner = new FileOutputRunner(runtime.getInstance(GcsOutputPlugin.class));
    }
    */

    @Test
    public void checkDefaultValues()
    {
        final ConfigSource config = GcsOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        PluginTask task = GcsOutputPlugin.CONFIG_MAPPER.map(config, PluginTask.class);
        assertEquals("private_key", task.getAuthMethod().toString());
    }

    // p12_keyfile is null when auth_method is private_key
    @Test
    public void checkDefaultValuesP12keyNull() throws IOException
    {
        final ConfigSource config = GcsOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", null)
                .set("formatter", formatterConfig());

        final Path out = embulk.createTempFile("csv");
        try {
            final TestingEmbulk.RunResult result = embulk.runOutput(config, Paths.get(LOCAL_PATH_PREFIX));
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            assertTrue(ex.getCause().getMessage().startsWith("If auth_method is private_key, you have to set both service_account_email and p12_keyfile"));
            return;
        }
        fail("Expected Exception was not thrown.");
    }

    // both p12_keyfile and p12_keyfile_path set
    @Test
    public void checkDefaultValuesConflictSetting() throws IOException
    {
        final ConfigSource config = GcsOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("p12_keyfile_path", GCP_P12_KEYFILE)
                .set("formatter", formatterConfig());

        try {
            final TestingEmbulk.RunResult result = embulk.runOutput(config, Paths.get(LOCAL_PATH_PREFIX));
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            ex.printStackTrace();
            return;
        }
        fail("Expected Exception was not thrown.");
    }

    // invalid p12keyfile when auth_method is private_key
    @Test
    public void checkDefaultValuesInvalidPrivateKey() throws IOException
    {
        final ConfigSource config = GcsOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", "invalid-key.p12")
                .set("formatter", formatterConfig());

        try {
            final TestingEmbulk.RunResult result = embulk.runOutput(config, Paths.get(LOCAL_PATH_PREFIX));
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            ex.printStackTrace();
            return;
        }
        fail("Expected Exception was not thrown.");
    }

    // json_keyfile is null when auth_method is json_key
    @Test
    public void checkDefaultValuesJsonKeyfileNull() throws IOException
    {
        final ConfigSource config = GcsOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", null)
                .set("formatter", formatterConfig());

        try {
            final TestingEmbulk.RunResult result = embulk.runOutput(config, Paths.get(LOCAL_PATH_PREFIX));
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            ex.printStackTrace();
            return;
        }
        fail("Expected Exception was not thrown.");
    }

    /*
    @Test
    public void testGcsClientCreateSuccessfully()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        ConfigSource configSource = config();
        PluginTask task = configSource.loadConfig(PluginTask.class);
        Schema schema = configSource.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSource, schema, 0, new Control());

        Method method = GcsOutputPlugin.class.getDeclaredMethod("createClient", PluginTask.class);
        method.setAccessible(true);
        method.invoke(plugin, task); // no errors happens
    }

    @Test(expected = ConfigException.class)
    public void testGcsClientCreateThrowConfigException()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        final ConfigSource config = GcsOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "gcs")
                .set("bucket", "non-exists-bucket")
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("auth_method", "json_key")
                .set("service_account_email", GCP_EMAIL)
                .set("json_keyfile", GCP_JSON_KEYFILE)
                .set("formatter", formatterConfig());

        PluginTask task = GcsOutputPlugin.CONFIG_MAPPER.map(config, PluginTask.class);

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(config, schema, 0, new Control());

        Method method = GcsOutputPlugin.class.getDeclaredMethod("createClient", PluginTask.class);
        method.setAccessible(true);
        try {
            method.invoke(plugin, task);
        }
        catch (InvocationTargetException ex) {
            throw (ConfigException) ex.getCause();
        }
    }

    @Test
    public void testResume()
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        plugin.resume(task.dump(), 0, new FileOutputPlugin.Control()  // no errors happens
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(GcsOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
    }

    @Test
    public void testCleanup()
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test
    public void testGcsFileOutputByOpen() throws Exception
    {
        ConfigSource configSource = config();
        PluginTask task = configSource.loadConfig(PluginTask.class);
        Schema schema = configSource.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSource, schema, 0, new Control());

        TransactionalFileOutput output = plugin.open(task.dump(), 0);

        output.nextFile();

        FileInputStream is = new FileInputStream(LOCAL_PATH_PREFIX);
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        output.add(buffer);

        output.finish();
        output.commit();

        String remotePath = GCP_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 1) + task.getFileNameExtension();
        assertRecords(remotePath);
    }

    @Test
    public void testGenerateRemotePath() throws Exception
    {
        ConfigSource configSource = config();
        PluginTask task = configSource.loadConfig(PluginTask.class);
        Method method = GcsTransactionalFileOutput.class.getDeclaredMethod("generateRemotePath", String.class, String.class, int.class, int.class, String.class);
        method.setAccessible(true);
        assertEquals("sample.000.01.csv", method.invoke(plugin, "/sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", method.invoke(plugin, "./sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", method.invoke(plugin, "../sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", method.invoke(plugin, "//sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("path/to/sample.000.01.csv", method.invoke(plugin, "/path/to/sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("path/to/./sample.000.01.csv", method.invoke(plugin, "path/to/./sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("path/to/../sample.000.01.csv", method.invoke(plugin, "path/to/../sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", method.invoke(plugin, "....../sample", task.getSequenceFormat(), 0, 1, ".csv"));
        assertEquals("sample.000.01.csv", method.invoke(plugin, "......///sample", task.getSequenceFormat(), 0, 1, ".csv"));
    }

    public ConfigSource config()
    {
        return GcsOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "gcs")
                .set("bucket", GCP_BUCKET)
                .set("path_prefix", GCP_PATH_PREFIX)
                .set("last_path", "")
                .set("file_ext", ".csv")
                .set("auth_method", "private_key")
                .set("service_account_email", GCP_EMAIL)
                .set("p12_keyfile", GCP_P12_KEYFILE)
                .set("json_keyfile", GCP_JSON_KEYFILE)
                .set("application_name", GCP_APPLICATION_NAME)
                .set("formatter", formatterConfig());
    }

    private class Control
            implements OutputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource)
        {
            return Lists.newArrayList(GcsOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport());
        }
    }
    */

    private ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", LOCAL_PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        return builder.build();
    }

    private ImmutableMap<String, Object> formatterConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("header_line", "false");
        builder.put("timezone", "Asia/Tokyo");
        return builder.build();
    }

    /*
    private void assertRecords(String gcsPath) throws Exception
    {
        ImmutableList<List<String>> records = getFileContentsFromGcs(gcsPath);
        assertEquals(5, records.size());
        {
            List<String> record = records.get(1);
            assertEquals("1", record.get(0));
            assertEquals("32864", record.get(1));
            assertEquals("2015-01-27 19:23:49", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk", record.get(4));
        }

        {
            List<String> record = records.get(2);
            assertEquals("2", record.get(0));
            assertEquals("14824", record.get(1));
            assertEquals("2015-01-27 19:01:23", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk jruby", record.get(4));
        }
    }

    private ImmutableList<List<String>> getFileContentsFromGcs(String path) throws Exception
    {
        ConfigSource config = config();

        PluginTask task = GcsOutputPlugin.CONFIG_MAPPER.map(config, PluginTask.class);

        Method method = GcsOutputPlugin.class.getDeclaredMethod("createClient", PluginTask.class);
        method.setAccessible(true);
        Storage client = (Storage) method.invoke(plugin, task);
        Storage.Objects.Get getObject = client.objects().get(GCP_BUCKET, path);

        ImmutableList.Builder<List<String>> builder = new ImmutableList.Builder<>();

        InputStream is =  getObject.executeMediaAsInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = reader.readLine()) != null) {
            List<String> records = Arrays.asList(line.split(",", 0));

            builder.add(records);
        }
        return builder.build();
    }
    */

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
