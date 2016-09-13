package rfpt;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.blockfile.cache.LruBlockCache;
import org.apache.accumulo.core.file.blockfile.cache.LruBlockCache.CacheStats;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class RandomSeekTest {

  private static final byte[] E = new byte[] {};
  private static final byte[] FAM = "pinky".getBytes();

  private static final int NUM_ROWS = 100_000;
  private static final int NUM_COLS = 100;

  public static void main(String[] args) throws Exception {

    int iterations;

    if (args.length == 0) {
      iterations = 100;
    } else if (args.length == 1) {
      iterations = Integer.parseInt(args[0]);
    } else {
      System.out.println("Usage " + RandomSeekTest.class.getName() + " [iterations]");
      return;
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    AccumuloConfiguration acuconf = AccumuloConfiguration.getDefaultConfiguration();

    String file = "/tmp/test.rf";
    if (!fs.exists(new Path(file))) {
      write(conf, fs, acuconf, file);
    }

    LruBlockCache indexCache = new LruBlockCache(1000000, 100000);
    LruBlockCache dataCache = new LruBlockCache(500000000, 100000);

    for (int i = 0; i < iterations; i++) {
      randomseeks(conf, fs, acuconf, file, dataCache, indexCache);
    }

    print(indexCache.getStats());
    print(dataCache.getStats());

    indexCache.shutdown();
    dataCache.shutdown();
  }

  private static void print(CacheStats stats) {
    System.out
        .println(stats.getRequestCount() + " " + stats.getHitCount() + " " + stats.getMissCount());

  }

  private static void randomseeks(Configuration conf, FileSystem fs, AccumuloConfiguration acuconf,
      String file, LruBlockCache dataCache, LruBlockCache indexCache) throws IOException {
    Stopwatch sw = new Stopwatch();

    sw.start();
    CachableBlockFile.Reader _cbr =
        new CachableBlockFile.Reader(fs, new Path(file), conf, dataCache, indexCache, acuconf);
    Reader reader = new RFile.Reader(_cbr);

    Random rand = new Random();
    for (int num = 0; num < 1000; num++) {
      int r = rand.nextInt(NUM_ROWS);
      int c = rand.nextInt(NUM_COLS);

      byte[] row = FastFormat.toZeroPaddedString(r, 8, 16, E);
      byte[] qual = FastFormat.toZeroPaddedString(c, 4, 16, E);

      Range range = Range.exact(new Text(row), new Text(FAM), new Text(qual));
      reader.seek(range, Collections.emptySet(), false);
      if (!reader.hasTop()) {
        throw new RuntimeException("whars my dater");
      }
    }

    sw.stop();

    reader.close();

    System.out.println(sw.elapsed(TimeUnit.MILLISECONDS));
  }

  private static void write(Configuration conf, FileSystem fs, AccumuloConfiguration acuconf,
      String file) throws IOException {
    FileSKVWriter writer = RFileOperations.getInstance().openWriter(file, fs, conf, acuconf);

    writer.startDefaultLocalityGroup();

    Random rand = new Random();


    for (int r = 0; r < NUM_ROWS; r++) {
      byte[] row = FastFormat.toZeroPaddedString(r, 8, 16, E);
      for (int c = 0; c < NUM_COLS; c++) {
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
