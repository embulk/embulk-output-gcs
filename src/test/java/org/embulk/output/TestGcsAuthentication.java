package org.embulk.output;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.storage.Storage;

import org.embulk.EmbulkTestRuntime;

import org.embulk.config.ConfigException;
import org.embulk.util.config.units.LocalFile;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
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
        LocalFile p12 = LocalFile.ofContent(System.getenv("GCP_P12_KEYFILE"));
        LocalFile json = LocalFile.ofContent(System.getenv("GCP_JSON_KEYFILE"));
        GCP_EMAIL = Optional.of(System.getenv("GCP_EMAIL"));
        GCP_P12_KEYFILE = Optional.of(p12.getPath().toString());
        GCP_JSON_KEYFILE = Optional.of(json.getPath().toString());
        GCP_BUCKET = System.getenv("GCP_BUCKET");
        // skip test cases, if environment variables are not set.
        assumeNotNull(GCP_EMAIL, GCP_P12_KEYFILE, GCP_JSON_KEYFILE, GCP_BUCKET);
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testGetServiceAccountCredentialSuccess()
            throws NoSuchFieldException, IllegalAccessException, GeneralSecurityException, IOException
    {
        GcsAuthentication auth = new GcsAuthentication(
                "json_key",
                GCP_EMAIL,
                GCP_P12_KEYFILE,
                GCP_JSON_KEYFILE,
                GCP_APPLICATION_NAME
        );

        Field field = GcsAuthentication.class.getDeclaredField("credentials");
        field.setAccessible(true);

        assertEquals(GoogleCredential.class, field.get(auth).getClass());
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetServiceAccountCredentialThrowFileNotFoundException()
            throws GeneralSecurityException, IOException
    {
        Optional<String> notFoundP12Keyfile = Optional.of("/path/to/notfound.p12");
        GcsAuthentication auth = new GcsAuthentication(
                "private_key",
                GCP_EMAIL,
                notFoundP12Keyfile,
                null,
                GCP_APPLICATION_NAME
        );
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetServiceAccountCredentialFromJsonThrowFileFileNotFoundException()
            throws GeneralSecurityException, IOException
    {
        Optional<String> notFoundJsonKeyfile = Optional.of("/path/to/notfound.json");
        GcsAuthentication auth = new GcsAuthentication(
                "json_key",
                GCP_EMAIL,
                null,
                notFoundJsonKeyfile,
                GCP_APPLICATION_NAME
        );
    }

    @Test
    public void testGetServiceAccountCredentialFromJsonSuccess() throws Exception
    {
        GcsAuthentication auth = new GcsAuthentication(
                "json_key",
                GCP_EMAIL,
                null,
                GCP_JSON_KEYFILE,
                GCP_APPLICATION_NAME
        );

        Storage client = auth.getGcsClient(GCP_BUCKET, 3);

        assertEquals(Storage.class, client.getClass());
    }

    @Test(expected = ConfigException.class)
    public void testGetServiceAccountCredentialFromJsonThrowConfigException() throws Exception
    {
        GcsAuthentication auth = new GcsAuthentication(
                "json_key",
                GCP_EMAIL,
                null,
                GCP_JSON_KEYFILE,
                GCP_APPLICATION_NAME
        );

        Storage client = auth.getGcsClient("non-exists-bucket", 3);
    }
}
