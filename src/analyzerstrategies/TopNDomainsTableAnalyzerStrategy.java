package logpp.analyzerstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;

public class TopNDomainsTableAnalyzerStrategy extends TopNDomainsTableAbstractStrategy {
  // Used to recognise analyzer files.
  private DateProjector inputDateProjector;
  private int n;

  public TopNDomainsTableAnalyzerStrategy(int n, DateProjector inputDateProjector, Filter filter) {
    super(filter);

    this.inputDateProjector = inputDateProjector;
    this.n = n;
  }

  public int finalN() {
    return this.n;
  }

  public String getInputFileRegex() {
    return String.format("\\d\\d\\d\\d-\\d\\d\\-%s-domain.csv-chunks", this.inputDateProjector.getName());
  }

  public int preN() {
    return n * 1000;
  }

  public String fileName() {
    return String.format("%s-top-%d-%s-domains.csv", this.inputDateProjector.getName(), this.finalN(), this.filter.getName());
  }
}
