package io.anserini.document.twitter;

import io.anserini.index.twitter.TweetAnalyzer;
import io.anserini.search.MicroblogTopic;
import io.anserini.search.MicroblogTopicSet;
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

public class DumpMicroblogQueriesToPlainText {

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input topics")
    public String input;

    @Option(name = "-output", metaVar = "[file]", required = true, usage = "output queries")
    public String output;
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

    MicroblogTopicSet topics = MicroblogTopicSet.fromFile(new File(args.input));

    FileWriter out = new FileWriter(args.output);
    for (MicroblogTopic topic : topics) {
      out.write(topic.getId() + " " + joiner.join(AnalyzerUtils.tokenize(analyzer, topic.getQuery())) + "\n");
    }
    out.close();
  }
}
