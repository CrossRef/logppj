# LogPPJ

Log Pre-Processor (in Java) takes DOI resolution logs (e.g. from CNRI) and processes them into aggregates for use elsewhere. Works with normal files (some gzipped), no external serivce or library dependencies. Crossref log files weigh in 100GB per year compressed, 500 GB uncompressed. Processing a month's worth of files, which adds up to 400,000,000 lines can be done in a few hours. A whole year (5 billion lines) can be done overnight.

# How to use LogPPJ

LogPPJ has three stages, and once one stage is completed the input files for that stage can be removed. 

# Stages

Output is in one of two formats:

 - `.csv` - normal CSV with dates as first column and other values as other series
 - `.csv-chunk` - many CSV tables in one file, separated by blank spaces. First line of each is the header line that identifies that chunk. For `day-domain.csv-chunks` the header line would be the domain.

## 1: Pre-process log files

Log files come from CNRI and look something like `access_log_201501_ec2.gz`. They correspond *almost* to the given month, but the servers are in arbitrary timezones, so there's often some spill into the month before or after. When the `preprocess` command is run the log files are converted into the `processed` format. Output files are not overwritten, but the all the input files are read every time.

Input: Gzipped CNRI log files in `/logs`, roughly corresponding to one month each. For Crossref, input is roughly 100GB per year. Input file format is one entry per resolution.

Output: Gzipped processed files in `/processed` corresponding *exactly* to one month per file. Output is roughly 30GB per year. Output format is one entry per resolution, tab-separated:

    date        doi                             code  full domain name        subdomain  domain
    2015-01-01  10.1016/j.jcrimjus.2010.04.019  H     sfx.carli.illinois.edu  sfx.carli  illinois.edu

Code classifies the refferer type. It is one of:

 -  `H` for HTTP protocol
 -  `S` for HTTPS protocol
 -  `F` for FTP protocol.
 -  `L` for file protocol.
 -  `U` for unknown protocol, but domain supplied
 -  `N` for no information.
 -  `W` for weird (e.g. readcube)

Hostname can be `unknown.special` to represent an unknown domain.

Weirdnesses: 

 - Dates are in local timezones, so a log file for Feburary may contain dates from January or March UTC. 
 - Log files are found in at least 2 different formats, one missing referrers.
 - Dates are round in at least 3 different formats.
 - Refferers are up to the browser. So all kinds of weird stuff is found in there (take a look at `Parser.java` for examples).

## 2: Aggregate processed files

The processed log files, which correspond to one per month, are aggregated to count various features. This creates an output file, one per month per type.

Input: Gzipped processed files in `/processed`

Output: CSV chunk files in `/aggregated` that look like `2015-01-day-code.csv-chunks`. The output is small, of the over of 100MB for Crossref. 

CSV Chunk files have a header line followed by date,count CSV lines followed by a blank line. The types of data are currently:

 - `code`, referrer code, defined above
 - `fulldomain`, the full domain, e.g. `en.wikipedia.org`
 - `domain`, the effective top level domain, e.g. `wikipedia.org`

## 3: Analysis

The aggreated files are combined into usable outputs, stored in `/analysis`. This a loosely defined stage, but where aggregated files are per month, analysis output files refer to 'all time'. Files are always over-written, but this stage is the cheapest (under a minute for a year's input).

The raw data is useful, but sometimes we want to exclude certain domains (e.g. data fed into Crossref Chronograph). In this case you can put the list of domains and/or subdomains in `filter-domain-names.txt` and `filter-full-domain-names.txt` in `/path/to/base/dir/`. Logppj automatically ignores domains like 'unknown.special' and 'doi.org' in the filtered analyzer output files.

You can grab a copy of the above files for Crossref from `http://destinations.labs.crossref.org/data/full-domain-names.txt` and `http://destinations.labs.crossref.org/data/domain-names.txt`.

 - `day-code.csv` - a big table of referrer codes per day. Codes are headers. One table because set of codes is known and small.
 - `day-domain-filtered.csv-chunks` - CSV chunks of count per day, one per domain (e.g. `wikiedia.org`). Chunks because the set of domains is unknown and large.
 - `day-domain-unfiltered.csv-chunks` - CSV chunks of count per day, one per domain (e.g. `wikiedia.org`). Chunks because the set of domains is unknown and large. Filters excluded domains.
 - `day-top-10-unfiltered-domains.csv` - a big table of the top 10 domains per day. More than 10 columns because it takes the union of all domains that were in the top 10 on any day.
 - `day-top-10-filtered-domains.csv` - a big table of the top 10 domains per day. More than 10 columns because it takes the union of all domains that were in the top 10 on any day. Filters excluded domains.
 - `month-code.csv` - a big table of referrer codes per month.
 - `month-top-10-unfiltered-domains.csv` - as `day-top-10-domains.csv` per month
 - `month-top-10-filtered-domains.csv` - as `day-top-10-domains.csv` per month, excluding filtered domains.

# 4: Distribtion

The analysis files are chopped up for use downstream, for example Chronograph. This stage doesn't get you anything except chopped up files. See `Main.java`.

# How to use it

 - Make sure Java 1.8 and ant are installed.
 - Run `ant jar`
 - Put `http://destinations.labs.crossref.org/data/full-domain-names.txt` in `/path/to/base/dir/ignore-full-domain-names.txt` if you want.
 - Put `http://destinations.labs.crossref.org/data/domain-names.txt` in `/path/to/base/dir/ignore-domain-names.txt` if you want.

Then:

  1. Put at least 3 months' log files in `/path/to/base/dir/logs`
  2. Run `java -jar dist/Main.jar process /path/to/base/dir` to parse those log files. Takes a couple of hours.
  3. Run `java -jar dist/Main.jar aggregate /path/to/base/dir`. Takes a couple of hours.
  4. Run `java -jar dist/Main.jar analyze /path/to/base/dir`. Takes 12 seconds.
  5. Enjoy output in `/path/to/base/dir/analyzed`
  6. If more log files come in:
    1. Put new month's log files in in `/path/to/base/dir/logs`
    2. Remove log files you've already processed. Remember to keep one month either side of the one you're intersted in.
    3. Remove spill-over files from `/path/to/base/dir/processed` for partial months

Note: 

 - Always include a month either side of the month you're interested in because of timezone spillover.
 - Processed files aren't overwritten, so delete relevant files in `/processed` if they exist. NB last month's spill-over files.
 - Once a month has been processed, you can remove the input log file, but remember if you want to process last month's log files you'll need to have kept the files from the month before and after it.
 - Aggregated files aren't overwritten. You can run the `aggregated` command at any time and it will only recalculate the data that hasn't already been calculated.
 - Don't delete aggregated files, they're all needed for the next stage.

# Effective TLD

Referrer domains (when they are known), e.g. "http://en.wikipedia.org" are split into domain, e.g. "wikipedia.org" and subdomain, e.g. "en". These are represented in the output as "full domain", e.g. "en.wikipedia.org" and "domain", e.g. "wikipedia.org". This is done using the `public_suffix_list.dat` which is included here and available at `https://publicsuffix.org/list/public_suffix_list.dat`. This list is useful rather than precise. It will split "abc.def.co.uk" into "abc.co.uk", but also has some convenience entries, so that e.g. "github.io" and "blogspot.com" are treated as eTLDs. This should be borne in mind.

# See also

The voracious reader might want to consult [Handle manual](http://www.handle.net/tech_manual/HN_Tech_Manual_8.pdf), section 3.7.1 . 


# Development notes

Can be run directly by ant: `time ant run  -Darg0=analyze -Darg1=/data-in/logs`.

For both the Aggregator and the Analyzer stages, the input is split into partitions. For every Aggregator/Analyzer Strategy, the input is run through as many times as there are partitions. These are done serially rather than in parallel because the partition size is designed to use as much RAM as possible without swapping. It's better to do a good chunk at once that's as large as possible than lots of smaller ones in parallel.

Heuristics based on Crossref's data. For DataCite's data the numbers will be different, but the amount of data will be much lower anyway, so it doesn't much matter.

The `process` stage is also done in serial because due to the timezones input files are multiplexed to output files. As initial processing is a low-volume activity there was no need to make everything threadsafe. Many classes are stateful and not threadsafe (because they don't have to be) in order to achieve speedups:

 - `LineParser` chooses the best format for parsing the lines and remembers it
 - `DateParser` chooses the best time format and remembers it
 - `DateParser` remembers the last date in order to avoid having to re-parse each in a series of dates that occur on the day
 - `ETLD` has a cache of domain lookups
 - `AggregatorStrategy` is stateful in that it has a counter object and the above parsers. They have a 'reset' function. Parallelizing would require creation of a further layer of `AggregatorStrategyAbstractFactory`s...

 Dates are stored as Strings. There is a whole constellation of types of date representations available in the JRE but we're only interested in YYYY-MM-DD in UTC.

 