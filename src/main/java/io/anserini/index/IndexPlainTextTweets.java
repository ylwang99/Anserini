package io.anserini.index;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class IndexPlainTextTweets {
  private static final Logger LOG = LogManager.getLogger(IndexPlainTextTweets.class);

  public static final Analyzer ANALYZER = new WhitespaceAnalyzer();
  public static String corpusFormat = null;

  private IndexPlainTextTweets() {}

  public static class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "output input path")
    public String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    public String collection;

    @Option(name = "-docids", metaVar = "[path]", required = false, usage = "docids file path")
    public String docids;
  }

  public static void main(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }

    final FieldType textOptions = new FieldType();
    textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    textOptions.setStored(false);
    textOptions.setTokenized(true);        

    LOG.info("collection: " + args.collection);
    LOG.info("docids: " + args.docids);
    LOG.info("index: " + args.index);
    
    long startTime = System.currentTimeMillis();

    Int2LongOpenHashMap tweetids = new Int2LongOpenHashMap();
    FileReader f = new FileReader(args.docids);
    BufferedReader reader = new BufferedReader(f);
    String line = null;
    while((line = reader.readLine()) != null) {
      String[] parts = line.split("\\s", 2);
      tweetids.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
    }
    reader.close();
    f.close();
    
    f = new FileReader(args.collection);
    reader = new BufferedReader(f);

    Directory dir = FSDirectory.open(Paths.get(args.index));
    final IndexWriterConfig config = new IndexWriterConfig(ANALYZER);

    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

    IndexWriter writer = new IndexWriter(dir, config);
    int cnt = 0;
    try {
      while((line = reader.readLine()) != null) {
        String[] parts = line.split("\\s", 2);
        int num = Integer.parseInt(parts[0]);
        String text = parts[1];

        if (!tweetids.containsKey(num)) {
          throw new RuntimeException("tweetid not found!");
        }

        cnt++;
        Document doc = new Document();
        doc.add(new StringField("id", tweetids.get(num) + "", Store.YES));
        doc.add(new Field("text", text, textOptions));
        
        writer.addDocument(doc);
        if (cnt % 100000 == 0) {
          LOG.info(cnt + " statuses indexed");
        }
      }

      LOG.info(String.format("Total of %s statuses added", cnt));
      
      LOG.info("Merging segments...");
      writer.forceMerge(1);
      LOG.info("Done!");

      LOG.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      writer.close();
      dir.close();
      f.close();
      reader.close();
    }
  }
}
