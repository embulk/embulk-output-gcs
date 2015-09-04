# Google Cloud Storage output plugin for Embulk

Google Cloud Storage output plugin for [Embulk](https://github.com/embulk/embulk).

## Overview

* **Plugin type**: file output
* **Load all or nothing**: no
* **Resume supported**: yes
* **Cleanup supported**: no

## Configuration

- **bucket**: Google Cloud Storage bucket name (string, required)
- **path_prefix**: Prefix of output keys (string, required)
- **file_ext**: Extention of output file (string, required)
- **content_type**: content type of output file (string, optional, default value is "application/octet-stream")
- **auth_method**: Authentication method `private_key` or `compute_engine` (string, optional, default value is "private_key")
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
  auth_method: `private_key` #default
  service_account_email: 'XYZ@developer.gserviceaccount.com'
  p12_keyfile_path: '/path/to/private/key.p12'
  formatter:
    type: csv
    encoding: UTF-8
```

## Authentication

There are two methods supported to fetch access token for the service account.

1. Public-Private key pair
2. Pre-defined access token (Compute Engine only)

The examples above use the first one.  You first need to create a service account (client ID),
download its private key and deploy the key with embulk.

On the other hand, you don't need to explicitly create a service account for embulk when you
run embulk in Google Compute Engine. In this second authentication method, you need to
add the API scope "https://www.googleapis.com/auth/devstorage.read_write" to the scope list of your
Compute Engine instance, then you can configure embulk like this.

[Setting the scope of service account access for instances](https://cloud.google.com/compute/docs/authentication)

```yaml
out:
  type: gcs
  auth_method: compute_engine
```

## Build

```
$ ./gradlew gem
```
