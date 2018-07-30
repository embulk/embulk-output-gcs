package org.embulk.output;

import com.google.api.services.storage.Storage;
import com.google.common.base.Function;
import com.google.common.base.Optional;
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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

public class GcsOutputPlugin implements FileOutputPlugin
{
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

    private GcsAuthentication newGcsAuth(PluginTask task)
    {
        try {
            return new GcsAuthentication(
                    task.getAuthMethod().getString(),
                    task.getServiceAccountEmail(),
                    task.getP12Keyfile().transform(localFileToPathString()),
                    task.getJsonKeyfile().transform(localFileToPathString()),
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
