package logpp.analyzerstrategies;

import logpp.*;

import java.io.IOException;
import java.io.Writer;

public class TopNDomainsTableAnalyzerStrategy extends TopNDomainsTableAbstractStrategy {
  // Used to recognise analyzer files.
  private DateProjector inputDateProjector;
  private int n;


  public TopNDomainsTableAnalyzerStrategy(int n, DateProjector inputDateProjector, DomainIgnorer ignorer) {
    super(ignorer);

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
    if (this.ignorer == null) {
      return String.format("%s-top-%d-all-domains.csv", this.inputDateProjector.getName(), this.finalN());
    } else {
      return String.format("%s-top-%d-ignored-domains.csv", this.inputDateProjector.getName(), this.finalN());
    }
  }
}
