package io.anserini.document.twitter;

import io.anserini.index.twitter.TweetAnalyzer;
import io.anserini.util.AnalyzerUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import com.google.common.base.Joiner;

public class DumpTweetsToPlainText {

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-collection", metaVar = "[file]", required = true, usage = "output file for the collection")
    public String collection;

    @Option(name = "-docids", metaVar = "[file]", required = true, usage = "output file for docids")
    public String docids;
  }

  public static void main(String[] argv) throws IOException {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }

    Analyzer analyzer = new TweetAnalyzer();
    Joiner joiner = Joiner.on(" ");

    FileWriter collection = new FileWriter(args.collection);
    FileWriter docids = new FileWriter(args.docids);

    int cnt = 0;
    JsonTweetsCollection tweets = new JsonTweetsCollection(new File(args.input));
    for (Status tweet : tweets) {
      cnt++;

      docids.write(cnt + " " + tweet.getId() + "\n");
      collection.write(cnt + " " + joiner.join(AnalyzerUtils.tokenize(analyzer, tweet.getText())) + "\n");

      if ( cnt % 100000 == 0) {
        System.out.println("Processed " + cnt + " tweets.");
      }
    }

    tweets.close();
    collection.close();
    docids.close();

    System.out.println("Processed " + cnt + " tweets in total.");
  }
}
