package logpp.etld;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

class Node {
  String name;
  // Leaves aren't always leaves.
  // e.g. both 'uk' and 'co.uk' are valid.
  boolean possibleLeaf = false;
  Map<String, Node> children = new HashMap<>();

  Node(String name) {
    this.name = name;
  }

  void markLeaf() {
    this.possibleLeaf = true;
  }

  // Add a domain recursively.
  void add(List<String> domain) {
    int length = domain.size();
    
    if (length == 0) {
      return;
    }

    String name = domain.get(length-1);
    List<String> rest = domain.subList(0, length-1);

    Node node = this.children.get(name);
    if (node == null) {
      node = new Node(name);
      this.children.put(name, node);
    }

    if (rest.isEmpty()) {
      node.markLeaf();
    } else {
      node.add(rest);
    }

  }

  void print() {
    System.out.println("***");
    print(0);
  }

  void print(int indent) {
    for (int i = 0 ; i < indent; i++) {
      System.out.print(" ");
    }

    System.out.format("%s %s\n", this.name, this.possibleLeaf);
    for (Map.Entry<String, Node> entry : this.children.entrySet()) {
      entry.getValue().print(indent + 1);
    }
  }

  String getDomain(String prefix, List<String> domain, boolean includeDomain) {
    // System.out.println("Prefix: " + prefix + " domain: " + domain);

    // If we've got here, by recursion or top-level, and there's none left, bail out.
    // This can happen for invalid domains, e.g. intranets
    // "http://wiki/wiki/display/intranet/Home;jsessionid=0BABB046E8BA084D51F92CDC1F4AF03D"
    if (domain.size() == 0) {
      return null;
    }

    int length = domain.size();
    String name = domain.get(0);
    List<String> rest = domain.subList(1, length);

    // System.out.println("Name: " + name);
    // System.out.println("Rest: " + rest);

    // Match name of child or a wildcard.
    Node child = this.children.get(name);
    if (child == null) {
      child = this.children.get("*");
    }

    // System.out.println("Child: " + child);

    // If we found a child, recurse.
    if (child != null) {
      // Don't join the empty top-level prefix with a ".".
      if (prefix == "") {
        return child.getDomain(name, rest, includeDomain);
      } else {
        return child.getDomain(name + "." + prefix, rest, includeDomain);
      }
    } else {
      // If this can't be a leaf then not allowed to stop here.
      if (this.possibleLeaf) {
        if (includeDomain) {
          return name + "." + prefix;
        } else {
          return prefix;
        }
      }
    }

    return null;
  }
}

public class ETLD {
  Node head = new Node("");

  // Cache of lookups that grows forever. The set of domains isn't huge.
  Map<String, String> domainCache = new HashMap<>();
  Map<String, String> eTldCache = new HashMap<>();

  public ETLD() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("public_suffix_list.dat.txt").getFile());

    try (Scanner scanner = new Scanner(file)) {

      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        if (line.startsWith("//")) {
          continue;
        }

        this.addLine(line);

      }
      scanner.close();

    } catch (IOException e) {
      e.printStackTrace();
    } 
  }

  private void quickTest() {
    System.out.println("TEST ETLD");
    String x;

    x = "image.baidu.com";
    System.out.println(x + " -> " + this.getDomain(x) + "," + this.getEtld(x) + ", " + Arrays.toString(this.getParts(x)));


    x = "buscatextual.cnpq.br";
    System.out.println(x + " -> " + this.getDomain(x) + "," + this.getEtld(x) + ", " + Arrays.toString(this.getParts(x)));

    x = "sfx.carli.illinois.edu";
    System.out.println(x + " -> " + this.getDomain(x) + "," + this.getEtld(x) + ", " + Arrays.toString(this.getParts(x)));

    x = "www.ncbi.nlm.nih.gov";
    System.out.println(x + " -> " + this.getDomain(x) + "," + this.getEtld(x) + ", " + Arrays.toString(this.getParts(x)));

    x = "www.oecd.org";
    System.out.println(x + " -> " + this.getDomain(x) + "," + this.getEtld(x) + ", " + Arrays.toString(this.getParts(x)));

    x = "aus.summon.serialssolutions.com";
    System.out.println(x + " -> " + this.getDomain(x) + "," + this.getEtld(x) + ", " + Arrays.toString(this.getParts(x)));

    System.exit(1);
  }

  public void addLine(String line) {
    List<String> domain = Arrays.asList(line.split("\\."));
    head.add(domain);
  }

  // For www.xyz.com return xyz.com
  private String split(String input, boolean includeDomain) {
    input = input.toLowerCase();
    List<String> domain = Arrays.asList(input.split("\\.", -1));
    Collections.reverse(domain);
    return this.head.getDomain("", domain, includeDomain);
  }

  public String getDomain(String input) {
    String result = this.domainCache.get(input);
    if (result != null) {
      return result;
    }

    result = this.split(input, true);
    this.domainCache.put(input, result);
    
    return result;
  }

  public String getEtld(String input) {
    String result = this.eTldCache.get(input);
    if (result != null) {
      return result;
    }

    result = this.split(input, false);
    this.eTldCache.put(input, result);
    
    return result;
  }

  // Return [subdomains, domain]
  public String[] getParts(String input) {
    String domain = this.getDomain(input);
    String subdomain;
    if (domain == null) {
      domain = "";
      subdomain = "";
    } else {
      // If there's a domain, remove it to get the subdomain, including the "." separator.
      // No subdomain means no separator, so skip.
      int snip = input.length() - domain.length() - 1;
      if (snip > 0) {
        subdomain = input.substring(0, snip);
      } else {
        subdomain = "";
      }
    }

    return new String[] {subdomain, domain};
  }
}
