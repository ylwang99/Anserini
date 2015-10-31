package io.anserini.search;

import java.util.List;

public class ScrapeTwitterSearch {
  public static void main(String[] args) throws Exception {
    TwitterSearchScraper scraper = new TwitterSearchScraper();
    List<String> results = scraper.query("birthday");

    for (String s : results) {
      System.out.println("--- Tweet ---");
      System.out.println(s);
    }
  }
}
