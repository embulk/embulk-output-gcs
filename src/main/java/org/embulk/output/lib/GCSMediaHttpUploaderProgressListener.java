/*
 * Copyright (c) 2011 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.embulk.output.lib;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;

import java.io.IOException;

/**
 * An interface for receiving progress notifications for uploads.
 *
 *
 */
public interface GCSMediaHttpUploaderProgressListener extends MediaHttpUploaderProgressListener
{
  /**
   * Called to notify that progress has been changed.
   *
   * <p>
   * This method is called once before and after the initiation request. For media uploads it is
   * called multiple times depending on how many chunks are uploaded. Once the upload completes it
   * is called one final time.
   * </p>
   *
   * <p>
   * The upload state can be queried by calling {@link MediaHttpUploader#getUploadState} and the
   * progress by calling {@link MediaHttpUploader#getProgress}.
   * </p>
   *
   * @param uploader Media HTTP uploader
   */
  public void progressChanged(GCSMediaHttpUploader uploader) throws IOException;
}
