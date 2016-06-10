# GDelt News Crawler
Crawls on a daily bases news articles that are indexed by the GDelt project (http://gdeltproject.org)

The program runs on a daily basis and crawls the news published one day before. The news are crawled from the events database provided by GDelt project on a daily basis (http://gdeltproject.org).

## Operations
There are four main steps:
<ul>
  <li>Daily Event Download http://data.gdeltproject.org/events/index.html</li>
  <li>Crawling of all indexed HTML news articles</li>
  <li>Boilerpipe execution on the extracted HTML documents</li>
  <li>Indexing into Solr the cleaned news articles (stripped off from HTML tags). The documents have as fields: {"url", "date", "title", "content"}.
</ul>

## Running the crawler

In order to run the GDelt crawler, there are only a very few parameters that need to be specified. We list them below.
<ul>
  <li>```-output_dir``` : Specifies the base directory where all the extracted content is stored.</li>
  <li>```-threads```: Specifies the number of threads with which to run the program. This is especially used when crawling the news articles in parallel. This value is dependent on the infrastructure used, a reasonable value is below 100.</li>
  <li>```-filter```: Specifies the path to a ```\n``` delimted file with the possible suffixes of an HTML document which you want to ignore from the process (e.g. PDF, MOV, MP4 etc.)</li>
  <li>```-min_year```: The minimum year from which to start crawling the GDelt data (the minimum possible year is 2013).</li>
  <li>```-min_month```: The minimum month from which to start crawling the GDelt data.</li>
  <li>```-min_day```: The minimum day from which to start crawling the GDelt data.</li>
  <li>```-server```: The Solr server URL where you want to index the crawled news articles.</li>
</ul>

An example run of the program can be the following ```java -cp gdelt_crawler.jar:lib/* GdeltCrawler -output_dir . -threads 10 -filter ignore_suffixes.txt -min_year 2016 -min_month 05 -min_day 01 -server http://YOUR_SOLR_SERVER````
