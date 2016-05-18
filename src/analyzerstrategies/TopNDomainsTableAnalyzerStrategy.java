package logpp.analyzerstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;

public class TopNDomainsTableAnalyzerStrategy extends TopNDomainsTableAbstractStrategy {
  // Used to recognise analyzer files.
  private DateProjector inputDateProjector;

  public TopNDomainsTableAnalyzerStrategy(DateProjector inputDateProjector) {
    this.inputDateProjector = inputDateProjector;
  }

  public int finalN() {
    return 10;
  }

  public String getInputFileRegex() {
    return String.format("\\d\\d\\d\\d-\\d\\d\\-%s-domain.csv-chunks", this.inputDateProjector.getName());
  }

  public int preN() {
    return 1000;
  }

  public String fileName() {
    return String.format("%s-top-%d-domains.csv", this.inputDateProjector.getName(), this.finalN());
  }
}
