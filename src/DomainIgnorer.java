package logpp;

import java.util.Set;
import java.util.HashSet;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;


// Domain ignorer.
// Tells callers if they can ignore a domain. Loads from text files, doesn't care if they don't exist.
public class DomainIgnorer {
  // All domains and subdomains.
  private Set<String> domains = new HashSet<>();

  // Things to get us started. If we want to exclude anything, we want to exclude these. 
  private String[] forStarters = new String[] {
    // This crops up. No idea why.
    "doi.org",
    "crossref.org",
    "unknown.special"
  };

  public DomainIgnorer(String domainPath, String fullDomainPath) throws FileNotFoundException, IOException {
    for (String domain: this.forStarters) {
      this.domains.add(domain);
    }

    File domainFile = new File(domainPath);
    File fullDomainFile = new File(fullDomainPath);

    this.read(domainFile);
    this.read(fullDomainFile);
  }

  private void read(File file) throws FileNotFoundException, IOException {
    if (!file.exists()) {
      System.out.format("Ignore domains file %s doesn't exist, skipping.\n", file.toString());
      return;
    }

    FileInputStream stream = new FileInputStream(file);
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    String line;

    while ((line = reader.readLine()) != null)   {
      this.domains.add(line.toLowerCase());
    }

    reader.close();
  }

  public boolean ignore(String domain) {
    return this.domains.contains(domain.toLowerCase());
  }
}