package com.expedia.fireflySupport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import sun.nio.ch.ChannelInputStream;

public class FileUtilities
{
  private static OpenOption options = readOptions;

  public BufferedReader newBufferedReader() {
    Reader reader = new InputStreamReader(newInputStream(), defaultCharset.newDecoder());
    return new BufferedReader(reader);
  }

  /**
   * Opens or creates a file for writing, returning a {@code BufferedWriter} that may be used to
   * write text to the file in an efficient manner. The {@code options} attribute specifies how the
   * the file is created or opened. If no options are present then this method works as if the
   * {@link StandardOpenOption#CREATE CREATE}, {@link StandardOpenOption#TRUNCATE_EXISTING
   * TRUNCATE_EXISTING}, and {@link StandardOpenOption#WRITE WRITE} options are present. In other
   * words, it opens the file for writing, creating the file if it doesn't exist, or initially
   * truncating an existing {@link #isRegularFile regular-file} to a size of {@code 0} if it exists.
   */
  public BufferedWriter newBufferedWriter() {
    Writer writer = new OutputStreamWriter(newOutputStream(), defaultCharset.newEncoder());
    return new BufferedWriter(writer);
  }

  /**
   * Opens or creates the designated file, returning a seekable byte channel to access the file.
   */
  public SeekableByteChannel newByteChannel(Set<? extends OpenOption> options) {
    try {
      return fs.provider().newByteChannel(base, options);
    } catch(IOException e) {
      dispatchException(e);
      return null;
    }
  }

  /**
   * Opens the designated file, returning an input stream to read from the file.
   */
  public InputStream newInputStream() {
    Set<? extends OpenOption> opts = options;
    if(opts != null) {
      if(!opts.equals(readOptions))
        throw new UnsupportedOperationException("'" + opts + "' not allowed");
    } else {
      opts = readOptions;
    }

    return new ChannelInputStream(newByteChannel(opts));
  }


}


