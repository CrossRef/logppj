package logpp.analyzerstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;

public class TopNDomainsPerYearTableAnalyzerStrategy extends TopNDomainsTableAbstractStrategy {
  public int finalN() {
    return 10;
  }

  public String getInputFileRegex() {
    return "\\d\\d\\d\\d-\\d\\d\\-month-domain.csv-chunks";
  }

  public int preN() {
    return 1000;
  }

  public String fileName() {
    return String.format("top-%d-year-domains.csv", this.finalN());
  }
}
