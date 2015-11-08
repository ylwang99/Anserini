package io.anserini.index;

import io.anserini.document.twitter.JsonStatusCorpusReader;
import io.anserini.document.twitter.Status;
import io.anserini.document.twitter.StatusStream;
import io.anserini.index.twitter.TweetAnalyzer;

import java.io.File;
import java.nio.file.Paths;
import java.text.DecimalFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

public class IndexTweetsRoundRobinPartitioned {
  private static final Logger LOG = LogManager.getLogger(IndexTweetsRoundRobinPartitioned.class);

  public static final Analyzer ANALYZER = new TweetAnalyzer();

  private static class MyArgs extends IndexArgs {
    @Option(name = "-partitions", metaVar = "[num]", required = true, usage = "number of partitions")
    int partitions;
  }

  private IndexTweetsRoundRobinPartitioned() {}

  public static void main(String[] args) throws Exception {
    MyArgs indexArgs = new MyArgs();
    CmdLineParser parser = new CmdLineParser(indexArgs, ParserProperties.defaults().withUsageWidth(90));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println("Example: IndexTweetsRoundRobinPartitioned" + parser.printExample(OptionHandlerFilter.REQUIRED));
      return;
    }

    LOG.info("Base index path: " + indexArgs.index);
    LOG.info("Optimize (merge segments): " + indexArgs.optimize);

    final FieldType textOptions = new FieldType();
    textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    textOptions.setStored(false);
    textOptions.setTokenized(true);

    DecimalFormat formatter = new DecimalFormat("00");
    Directory[] dirs = new Directory[indexArgs.partitions];
    IndexWriter[] writers = new IndexWriter[indexArgs.partitions];

    for (int i=0; i<indexArgs.partitions; i++) {
      String partDir = indexArgs.index + "/partition" + formatter.format(i);
      LOG.info("Creating index writer for " + partDir);
      IndexWriterConfig config = new IndexWriterConfig(ANALYZER);
      config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

      dirs[i] = FSDirectory.open(Paths.get(partDir));
      writers[i] = new IndexWriter(dirs[i], config);
    }

    long startTime = System.currentTimeMillis();
    StatusStream stream = new JsonStatusCorpusReader(new File(indexArgs.input));

    int cnt = 0;
    Status status;
    try {
      while ((status = stream.next()) != null) {
        if (status.getText() == null) {
          continue;
        }

        cnt++;
        Document doc = new Document();
        doc.add(new LongField("id", status.getId(), Field.Store.YES));
        doc.add(new Field("text", status.getText(), textOptions));

        writers[cnt % indexArgs.partitions].addDocument(doc);

        if (cnt % 100000 == 0) {
          LOG.info(cnt + " statuses indexed");
        }
      }

      LOG.info(String.format("Total of %s statuses added", cnt));

      if (indexArgs.optimize) {
        for (int i = 0; i < indexArgs.partitions; i++) {
          LOG.info("Merging segments for " + i + "...");
          writers[i].forceMerge(1);
          LOG.info("Done!");
        }
      }

      LOG.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      for (int i = 0; i < indexArgs.partitions; i++) {
        writers[i].close();
        dirs[i].close();
      }
      stream.close();
    }
  }
}
