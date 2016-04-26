# DOI Resolution Log Preprocessor

Takes log files from the DOI resolver servers and converts them to something a bit more usable and compact. Discards things we're not interested in, tidies up things that we are. Anonymises. Takes compressed log files as input.

 - normalizes timezones to UTC
 - truncates times to the day 
 - normalizes date-rotated filenames for timezone and multiplexes log files
 - normalizes referrer domains, discards path
 - records referrer protocol
 - tidies up weirdly encoded referrer domains
 - discards IP address, command, status
 - anonymises logs by removal of IP address, precise day, precise referrer URL

Takes lines like:

    123.123.123.123 HTTP:HDL "Mon Apr 01 12:00:01 EDT 2013" 1 1 34ms 10.1016/j.ijoa.2009.01.009 "200:0.na/10.1016" "http://example.com"

And turns them into:

    2013-04-01  10.1016/j.ijoa.2009.01.009  H example.com

For supervised R&D use!

# To use

Have a load of log files in gzip format, named like `access_log*.gz`. Have a directory to put processed log files into, empty or otherwise. Output files won't be overwritten. The processed files will be named after the month of the data in them, and multiple input log files for a given time period will be merged. Note that one input log file, nominally for one month, may not correspond exactly to that month. Thus small file one side or the other of the input log file may be created. If you want the processed log file for March, you will also need February and April for best results. **Existing processed log files will not be overwritten** so if you do have a small spilled file as a result of a previous run, you should delete it first.

Requires Java 1.8.

Compile:

    ant jar

Run:

    java -jar dist/Main.jar /Users/jwass/data/logs /Users/jwass/data/logs-processed

When developing:

    ant run -Darg0=/Users/jwass/data/logs -Darg1=/Users/jwass/data/logs-processed

Look at the output, it may contain something you're interested in. A log message is shown every million lines processed. Unparsable referrers are logged, which you may want to adjust the code to process.

# Output format

    «YYYY-MM-DD in UTC» «DOI» «code» «referring domain»

The referrer is sent by some browsers. It is the URL that the user was on when they clicked the DOI. In theory. In practice there is some variation, as any string could be sent. The following referrer types are defined:

 -  `H` for HTTP protocol
 -  `S` for HTTPS protocol
 -  `F` for FTP protocol.
 -  `L` for file protocol.
 -  `U` for unknown protocol, but domain supplied
 -  `N` for no information.
 -  `W` for weird (e.g. readcube)

Some "W" types may be accompanied by a special domain:

 - `readcube.special` for Readcube app
 - `file.special` for a locally saved file.

# Stats

Over the first two months of 2016, :

 - 16 GB input in compressed log files
 - corresponding to XX uncompressed characters, or XX lines
 - runs in XX minutes on my laptop (Macbook Pro 2015, 3.1GHz, 16GB RAM)
 - produces XX bytes of uncompressed output
 - XX failures

# Issues

Not all lines parse smoothly. In first 2 months of 2016 XX failed to parse, which is XX percent. This can be due to corrupted logs, DOIs with spaces in (there's no way to recover from this) or odd unrecognised events on the log.

The time format doesn't seem to conform to any international standard. Timezones names are ambigious and mappings are therefore hardcoded. Different standards are used in different files.

Unrecognised referrers are logged for possible adjustment of handling in code. Most are no-hopers. All other errors deliberately cause a crash.

# Further reading

The voracious reader might want to consult [Handle manual](http://www.handle.net/tech_manual/HN_Tech_Manual_8.pdf), section 3.7.1 . 


# Failing examples

## Failing referrers

Examples of failures. These happen less than 10 times each so there's no point writing exceptions for them.

    perComponent5.prototype.GetFramesCollectionLen%20(jar:file:///C:/Program%20Files%20(x86)/Internet%20Download%20Manager/idmmzcc2.xpi!/components/idmhelper5.js:173)
    
    read:http://especiais.gazetaonline.com.br/bomba/
      %C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5%C3%A5IDMCCHelperComponent5.prototype.GetLinks%20(jar:file:///C:/Program%20Files%20(x86)/Internet%20Download%20Manager/idmmzcc2.xpi!/components/idmhelper5.js:367)
    