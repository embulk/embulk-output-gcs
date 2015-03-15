# Google Cloud Storage output plugin for Embulk

**CAUTION: This plugin is currently experimental. DO NOT USE IN PRODUCTION**.

## Overview

* **Plugin type**: file output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **bucket**: Google Cloud Storage bucket name (string, required)
- **path_prefix**: Prefix of output keys (string, required)
- **file_ext**: Extention of output file (string, required)
- **content_type**: content type of output file (string, optional, default value is "application/octet-stream")
- **service_account_email**: Google Cloud Platform service account email (string, required)
- **p12_keyfile_path**: Private key file fullpath of Google Cloud Platform service account (string, required)
- **application_name**: Application name, anything you like (string, optional, default value is "embulk-output-gcs")

## Example

```yaml
out:
  type: gcs
  bucket: your-gcs-bucket-name
  path_prefix: logs/out
  file_ext: .csv
  service_account_email: 'XYZ@developer.gserviceaccount.com'
  p12_keyfile_path: '/path/to/private/key.p12'
  formatter:
    type: csv
    encoding: UTF-8
```

## Build

```
$ ./gradlew gem
```
