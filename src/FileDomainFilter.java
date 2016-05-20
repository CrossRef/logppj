package logpp;

import logpp.etld.ETLD;

import java.util.Set;
import java.util.HashSet;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;


// Domain filter.
// Tells callers if they can ignore a domain. Loads from text files, doesn't care if they don't exist.
public class FileDomainFilter implements Filter {
  // All domains and subdomains.
  private Set<String> domains = new HashSet<>();

  private ETLD etld = new ETLD();

  public FileDomainFilter(String[] domains, String[] domainFiles) throws FileNotFoundException, IOException {
    for (String domain: domains) {
      this.domains.add(domain);
    }

    for (String domainFileName : domainFiles) {
      this.read(new File(domainFileName));
    }

    System.out.format("Filter has %d domains\n", this.domains.size());
  }

  private void read(File file) throws FileNotFoundException, IOException {
    if (!file.exists()) {
      System.out.format("Filter domains file %s doesn't exist, skipping.\n", file.toString());
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

  public boolean keep(String domain) {
    domain = domain.toLowerCase();

    // Full domain match.
    if (this.domains.contains(domain)) {
      return false;
    }
    
    // Check if this domain is a subdomain of one on the list.
    if (this.domains.contains(this.etld.getDomain(domain))) {
      return false;
    }
    
    return true;
  }

  public String getName() {
    return "filtered";
  }
}