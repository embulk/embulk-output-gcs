Embulk::JavaPlugin.register_output(
  "gcs", "org.embulk.output.GcsOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
