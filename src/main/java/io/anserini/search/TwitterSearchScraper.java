package io.anserini.search;

import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

import com.google.common.collect.Lists;

public class TwitterSearchScraper {
  public TwitterSearchScraper() {}

  public List<String> query(String query) throws InterruptedException {
    WebDriver driver = new FirefoxDriver();
    List<String> results = Lists.newArrayList();

    driver.get("https://twitter.com/search?q=" + query + "&src=typd&lang=en");
    List<WebElement> streamContainer = driver.findElements(By.className("stream-container"));

    List<WebElement> items = streamContainer.get(0).findElements(By.className("js-stream-item"));
    for (WebElement element : items) {
      results.add(element.getText());
    }

    return results;
  }
}