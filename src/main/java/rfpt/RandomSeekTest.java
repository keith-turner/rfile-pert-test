package rfpt;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFile.ScannerOptions;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.PrintInfo;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Stopwatch;

public class RandomSeekTest {

  private static final byte[] E = new byte[] {};
  private static final byte[] FAM = "pinky".getBytes();


  public static class Options {
    @Parameter(names = {"-i", "--iterations"}, description = "Iterations to run test")
    public int iterations = 100;

    @Parameter(names = {"--noCache"}, description = "Disable cache")
    public boolean noCache = false;

    @Parameter(names = {"--idxBlockSize"}, description = "Index block size")
    public String idxBlockSize = null;

    @Parameter(names = {"--blockSize"}, description = "Data block size")
    public String dataBlockSize = null;

    @Parameter(names = {"--idxCacheSize"}, description = "Index block size")
    public String idxCacheSize = "10M";

    @Parameter(names = {"--cacheSize"}, description = "Data block size")
    public String dataCacheSize = "500M";

    @Parameter(names = {"--requery"}, description = "Percentage of time that a recent query will be redone instead of a new random query")
    public int requeryPercentage = 33;

    @Parameter(names = {"--rows"}, description = "Number of rows")
    public int numRows = 100_000;

    @Parameter(names = {"--cols"}, description = "Number of columns")
    public int numCols = 100;

    @Parameter(names = {"--preScan"}, description = "Prescan all data, will load it in cache if it all fits")
    public boolean preScan = false;
  }

  private static int[] requeryRows;
  private static int[] requeryCols;

  private static long parseSize(String size){
    if(size.endsWith("K")) {
      return Long.parseLong(size.substring(0, size.length()-1)) * 1024;
    }

    if(size.endsWith("M")) {
      return Long.parseLong(size.substring(0, size.length()-1)) * 1024 * 1024;
    }

    if(size.endsWith("G")) {
      return Long.parseLong(size.substring(0, size.length()-1)) * 1024 * 1024 * 1024;
    }

    return Long.parseLong(size);
  }

  public static void main(String[] args) throws Exception {

    Options opts = new Options();
    new JCommander(opts, args);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

   Map<String, String> props = new HashMap<>();


   props.put("tserver.cache.factory.class", "accumulo.ohc.OhcCacheManager");
   if(opts.dataBlockSize != null){
     props.put("table.file.compress.blocksize", opts.dataBlockSize);
   }

   if(opts.idxBlockSize != null){
     props.put("table.file.compress.blocksize.index", opts.idxBlockSize);
   }


   String file = String.format("/tmp/test_%d_%d_%s_%s.rf", opts.numRows, opts.numCols, opts.idxBlockSize, opts.dataBlockSize);

    if (!fs.exists(new Path(file))) {
      write(conf, fs, props, file, opts.numRows, opts.numCols);
    }

    PrintInfo.main(new String[]{"file://"+file});

    ScannerOptions builder = RFile.newScanner().from(file).withFileSystem(fs).withoutSystemIterators();
    if(!opts.noCache) {
      builder = builder.withIndexCache(parseSize(opts.idxCacheSize)).withDataCache(parseSize(opts.dataCacheSize)).withTableProperties(props);
    }
    Scanner scanner = builder.build();

    if(opts.preScan){
      scanner.forEach(e -> {});
    }

    int numRequery = 10_000;
    requeryRows = new int[numRequery];
    requeryCols = new int[numRequery];

    Random rand = new Random();
    for(int i = 0; i < numRequery; i++){
      requeryRows[i] = rand.nextInt(opts.numRows);
      requeryCols[i] = rand.nextInt(opts.numCols);
      seek(scanner, requeryRows[i], requeryCols[i]);
    }

    DescriptiveStatistics stats = new DescriptiveStatistics();
    for(int i = 0; i< opts.iterations; i++) {
      stats.addValue(randomseeks(scanner, opts.numRows, opts.numCols, opts.requeryPercentage));
    }

    scanner.close();

    System.out.println();
    System.out.println(stats.toString());
  }



  private static long randomseeks(Scanner scanner, int numRows, int numCols, int requeryPercent) throws IOException {
    Stopwatch sw = new Stopwatch();


    Random rand = new Random();

    sw.start();

    for (int num = 0; num < 1000; num++) {

      int r,c;

      if(num > 0 && rand.nextInt(100) < requeryPercent){
        //sometimes requery a recently queried row/col
        int i = rand.nextInt(requeryRows.length);
        r = requeryRows[i];
        c = requeryCols[i];
      } else {
        r = rand.nextInt(numRows);
        c = rand.nextInt(numCols);
      }

      seek(scanner, r, c);
    }

    sw.stop();

    long t = sw.elapsed(TimeUnit.MILLISECONDS);
    System.out.println(t);
    return t;
  }

  private static void seek(Scanner scanner, int r, int c) {
    byte[] row = FastFormat.toZeroPaddedString(r, 8, 16, E);
    byte[] qual = FastFormat.toZeroPaddedString(c, 4, 16, E);

    Range range = Range.exact(new Text(row), new Text(FAM), new Text(qual));
    scanner.setRange(range);
    Iterator<Entry<Key,Value>> iter = scanner.iterator();

    if (!iter.hasNext()) {
      throw new RuntimeException("whars my dater");
    }
  }

  private static void write(Configuration conf, FileSystem fs, Map<String, String> props, String file, int numRows, int numCols) throws Exception {

    RFileWriter writer = RFile.newWriter().to(file).withFileSystem(fs).withTableProperties(props).build();

    writer.startDefaultLocalityGroup();

    Random rand = new Random();

    for (int r = 0; r < numRows; r++) {
      byte[] row = FastFormat.toZeroPaddedString(r, 8, 16, E);
      for (int c = 0; c < numCols; c++) {
        byte[] qual = FastFormat.toZeroPaddedString(c, 4, 16, E);

        Key k = new Key(row, FAM, qual, E, 1L);
        byte[] val = new byte[32];
        rand.nextBytes(val);

        writer.append(k, new Value(val));
      }
    }

    writer.close();
  }

}
