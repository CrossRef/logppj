package logpp;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Writer;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;

public class ChunkHashDistributerStrategy implements DistributerStrategy, ChunkParserCallback {
  private String filename;
  private File inputDir;
  private File outputDir;
  private MessageDigest md5;
  
  // Bucket name to open writer for that bucket.
  private Map<String, Writer> handles = new HashMap<>();

  // Current bucket we're writing to.
  private Writer currentHandle;

  public ChunkHashDistributerStrategy(String inputDir, String outputDir, String filename) {
    this.filename = filename;
    this.inputDir = new File(inputDir);
    this.outputDir = new File(outputDir);
    try {
      this.md5 =  MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException ex) {
      // Bad news. Exception will happen soon. But on a known platform, this shouldn't happen...
      System.err.println("ERROR: Can't instantiate MD5");
    }
  }

  final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
  public static String bytesToHex(byte[] bytes) {
      char[] hexChars = new char[bytes.length * 2];
      for ( int j = 0; j < bytes.length; j++ ) {
          int v = bytes[j] & 0xFF;
          hexChars[j * 2] = hexArray[v >>> 4];
          hexChars[j * 2 + 1] = hexArray[v & 0x0F];
      }
      return new String(hexChars);
  }

  public void run() throws IOException {
    ChunkParser parser = new ChunkParser(this);
    
    BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(new File(inputDir, filename))));

    String line;
    while ((line = input.readLine()) != null) {
      parser.feed(line);
    }

    for (Map.Entry<String, Writer> handle : this.handles.entrySet()) {
      handle.getValue().close();
    }
  }

  private Writer getBucket(String input) throws IOException, UnsupportedEncodingException {
    byte[] digest = this.md5.digest(input.getBytes("UTF-8"));
    String result = bytesToHex(digest);
    String bucketName = filename + "_" + result.substring(0, 3);

    Writer handle = this.handles.get(bucketName);
    if (handle == null) {
      handle = new BufferedWriter(new FileWriter(new File(outputDir, bucketName)));
      this.handles.put(bucketName, handle);
    }

    return handle;
  }

  // ChunkParserCallback
  // Seen a header.
  public void header(String name) {
    try {
      // Finish off chunk in *last* bucket.
      if (this.currentHandle != null) {
        this.currentHandle.write("\n");
      }
      
      // Get a new bucket for this chunk.
      this.currentHandle = this.getBucket(name);

      // And start writing this chunk.
      this.currentHandle.write(name);
      this.currentHandle.write("\n");

    } catch (IOException ex) {
      System.err.println("Error: can't open bucket " + name);
    }
    
  }

  // ChunkParserCallback
  // Seen a line.
  public void line(String fields) {
    try {
      this.currentHandle.write(fields);
      this.currentHandle.write("\n");
    } catch (IOException ex) {
      System.err.println("Error: can't write to bucket.");
    }
  }
}