package rfpt;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.matchprocessor.SubclassMatchProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager;
import org.apache.accumulo.core.file.rfile.bcfile.PrintInfo;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Stopwatch;

public class RandomSeekTest {
  
  private static final byte[] E = new byte[] {};
  private static final byte[] FAM = "pinky".getBytes();
    
  private int iterations = 100;
  private boolean noCache = false;
  private String idxBlockSize, dataBlockSize, idxCacheSize, dataCacheSize = null;
  private int popularSize = 10_000;
  private int numRows = 100_000;
  private int numCols = 100;
  private boolean preScan = false;
  
  private String fileName = null;
  private FileSystem fs = null;
  private Map<String,String> props = null;
  
  @Before
  public void setup() throws Exception {
	CompositeConfiguration cc = new CompositeConfiguration();
	cc.addConfiguration(new SystemConfiguration());
	cc.addConfiguration(new PropertiesConfiguration("seek-test.properties"));

	iterations = cc.getInt("iterations", iterations);
	noCache = cc.getBoolean("noCache", noCache);
	idxBlockSize = cc.getString("idxBlockSize", "32K");
	dataBlockSize = cc.getString("blockSize", "32K");
	idxCacheSize = cc.getString("idxCacheSize", "1G");
	dataCacheSize = cc.getString("cacheSize", "1G");
	popularSize = cc.getInt("popular", popularSize);
	numRows = cc.getInt("rows", numRows);
	numCols = cc.getInt("cols", numCols);
	preScan = cc.getBoolean("preScan", preScan);

  }
    
  @Test
  public void testBlockCacheRandomSeek() throws Exception {
	  
	  final List<Class<? extends BlockCacheManager>> managers = new ArrayList<>();
	  FastClasspathScanner cpScanner = new FastClasspathScanner().matchSubclassesOf(BlockCacheManager.class, new SubclassMatchProcessor<BlockCacheManager>() {
		  @Override
		  public void processMatch(Class<? extends BlockCacheManager> clazz) {
			  System.out.println("Found class: " + clazz);
			  managers.add(clazz);
		  }
	  });
	  cpScanner.scan();

	  Assert.assertNotEquals(0, managers.size());
	  for (Class<? extends BlockCacheManager> manager : managers) {
			Configuration conf = new Configuration();
			conf.set("tserver.cache.manager.class", manager.getName());
			conf.set("tserver.cache.data.size", dataCacheSize);
			conf.set("tserver.cache.index.size", idxCacheSize);
			conf.set("tserver.default.blocksize", dataBlockSize);
			conf.set("table.file.compress.blocksize", dataBlockSize);
			conf.set("table.file.compress.blocksize.index", idxBlockSize);
			conf.set("general.custom.cache.block.tiered.expiration.time", "1");
			conf.set("general.custom.cache.block.tiered.expiration.time_units", "HOUR");
			conf.set("general.custom.cache.block.tiered.expiration.off-heap.max.size", "1G");
			conf.set("general.custom.cache.block.tiered.expiration.off-heap.block.size", "16K");

			props = new HashMap<>();
			conf.forEach(e -> { props.put(e.getKey(), e.getValue());});
			
		    fs = FileSystem.getLocal(conf);
		    fileName = String.format("test_%d_%d_%s_%s.rf", numRows, numCols, idxBlockSize, dataBlockSize);
		    if (!fs.exists(new Path(fileName))) {
		      write(conf, fs, props, fileName, numRows, numCols);
		    }
		    PrintInfo.main(new String[]{fileName});
		  
		  System.out.println("--------------------------------------------------------------------------------");
		  System.out.println(manager.getSimpleName());
		  System.out.println("--------------------------------------------------------------------------------");
		  
		  ScannerOptions builder = RFile.newScanner().from(fileName).withFileSystem(fs).withoutSystemIterators();
		  if(!noCache) {
			  builder = builder.withIndexCache(parseSize(idxCacheSize)).withDataCache(parseSize(dataCacheSize)).withTableProperties(props);
		  }
		  Scanner scanner = builder.build();
		  if(preScan){
			  int onePercent = Math.max(numRows * numCols / 100, 1);
			  int count = 0;
			  for (@SuppressWarnings("unused") Entry<Key,Value> entry : scanner) {
				  count++;
				  if(count % onePercent == 0) {
					  System.out.printf("Prescanned %,d\n", count);
				  }
			  }
		  }
		  int[] popularRows = new int[popularSize];
		  int[] popularCols = new int[popularSize];
		  
		  Random rand = new Random();
		  for(int i = 0; i < popularSize; i++){
			  popularRows[i] = rand.nextInt(numRows);
			  popularCols[i] = rand.nextInt(numCols);
		  }
		  
		  DescriptiveStatistics stats = new DescriptiveStatistics();
		  DescriptiveStatistics popularStats = new DescriptiveStatistics();
		  
		  for(int i = 0; i< iterations; i++) {
			  popularStats.addValue(popularseeks(scanner, popularRows, popularCols));
			  stats.addValue(randomseeks(scanner, numRows, numCols));
		  }
		  
		  scanner.close();
		  
		  System.out.println();
		  System.out.println(popularStats.toString());
		  System.out.println(stats.toString());
	  }
	  
	  
  }

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

  private static double popularseeks(Scanner scanner, int[] popularRows, int[] popularCols) {

    Stopwatch sw = new Stopwatch();

    Random rand = new Random();

    sw.start();

    for (int num = 0; num < 1000; num++) {
      int i = rand.nextInt(popularRows.length);
      seek(scanner, popularRows[i], popularCols[i]);
    }

    sw.stop();

    long t = sw.elapsed(TimeUnit.MILLISECONDS);
    System.out.println("popular    : "+t);
    return t;
  }

  private static long randomseeks(Scanner scanner, int numRows, int numCols) throws IOException {
    Stopwatch sw = new Stopwatch();

    Random rand = new Random();

    sw.start();

    for (int num = 0; num < 1000; num++) {

      int r,c;
      r = rand.nextInt(numRows);
      c = rand.nextInt(numCols);

      seek(scanner, r, c);
    }

    sw.stop();

    long t = sw.elapsed(TimeUnit.MILLISECONDS);
    System.out.println("everything : "+t);
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
