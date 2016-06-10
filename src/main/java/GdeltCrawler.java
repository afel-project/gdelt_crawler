import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import utils.FileUtils;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by besnik on 7/10/15.
 */
public class GdeltCrawler extends TimerTask {
    private static final SimpleDateFormat date_formatter = new SimpleDateFormat("yyyy-MM-dd");
    public static String finished_dates_files = "finished_gdelt_dates.csv";
    public static String gdelt_event_url_base = "http://data.gdeltproject.org/events/";

    public static Set<String> crawl_filter = new HashSet<>();
    public static int thread_no;

    public static String output_dir = "", min_year = "", min_month = "", min_day = "";

    /**
     * Downloads the events list database from the GDelt page. The event list contains a set of URLs. We first check
     * if for a given minimal date we have crawled them previously, otherwise we download from the minimal date
     * up to the current date.
     *
     * @param min_year  The minimum year that is used to initialize the crawler
     * @param min_month the minimum month that belongs to min year in order to initialize the crawler
     * @param min_day   the minimum day that belongs to min year and min-month in order to initialize the crawler
     */
    public void downloadEventData(String min_year, String min_month, String min_day, String out_dir) throws ParseException, IOException, InterruptedException, SolrServerException {
        String last_date = "";
        if (FileUtils.fileExists(finished_dates_files, true)) {
            String[] processed_dates = FileUtils.readText(finished_dates_files).split("\n");
            last_date = processed_dates[processed_dates.length - 1];
        }
        String min_date = min_year + "-" + min_month + "-" + min_day;
        //in case we do not have anything processed previously then use the min_date as the starting date for crawling.
        last_date = last_date.isEmpty() ? min_date : last_date;

        //check if the dates are crawled from min_date up to the current date.
        Calendar calendar = Calendar.getInstance();
        //set the time to the specific last date set for crawling.
        calendar.setTime(date_formatter.parse(last_date));
        if (!last_date.isEmpty()) {
            calendar.add(Calendar.DAY_OF_YEAR, 1);
        }
        //set the current date
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -1);
        Date current_date = cal.getTime();

        while (calendar.getTime().compareTo(current_date) <= 0) {
            //download the GDelt events file
            String date_raw = date_formatter.format(calendar.getTime());
            int year = Integer.valueOf(date_raw.substring(0, date_raw.indexOf("-")));
            String date = date_raw.replaceAll("-", "");
            String gdelt_events_file = gdelt_event_url_base + date + ".export.CSV.zip";

            gdelt_events_file = readGDeltEventsFile(gdelt_events_file, date, year, out_dir);
            System.out.printf("Finished downloading the GDelt event database for day %s\n", date);

            //crawl the news articles.
            crawlDailyGDeltEvents(gdelt_events_file, out_dir, date);
            System.out.printf("Finished crawling the GDelt news articles for day %s\n", date);

            //clean the crawled articles by stripping off the HTML tags.
            boilerPipeCleanCrawledURLs(out_dir, date, year);
            System.out.printf("Finished cleaning the crawled raw HTML pages for the GDelt news articles for day %s\n", date);

            //index the documents into SOLR
            indexIntoSOLR(out_dir, year, date);
            System.out.printf("Finished indexing the cleaned HTML pages for the GDelt news articles for day %s\n", date);

            FileUtils.saveText(date_raw + "\n", finished_dates_files, true);
            //increment the date by one day
            calendar.add(Calendar.DAY_OF_YEAR, 1);
        }
    }

    /**
     * Download the GDelt events file from the GDELT web page for a given date.
     *
     * @param file_url
     * @param date
     * @param out_dir
     * @throws IOException
     */
    public String readGDeltEventsFile(String file_url, String date, int year, String out_dir) throws IOException, InterruptedException {
        FileUtils.checkDir(out_dir + "/events/");
        FileUtils.checkDir(out_dir + "/events/" + year);
        String filename = out_dir + "/events/" + year + "/" + date + "_event_file.CSV.zip";

        if (FileUtils.fileExists(filename, false)) {
            return filename;
        }

        URL website = new URL(file_url);
        ReadableByteChannel rbc = Channels.newChannel(website.openStream());

        FileOutputStream fos = new FileOutputStream(filename);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        fos.flush();
        fos.close();
        return filename;

    }

    /**
     * For a given GDelt events file, crawl all the URLs. Store the URLs with on a file index.
     *
     * @param gdelt_event_file
     * @param output_dir
     * @param date
     * @throws InterruptedException
     */
    public void crawlDailyGDeltEvents(String gdelt_event_file, String output_dir, String date) throws InterruptedException {
        FileUtils.checkDir(output_dir + "/file_index/");
        String index_file = output_dir + "/file_index/index_file_" + date + ".csv";

        String[] crawl_urls = FileUtils.readText(gdelt_event_file).split("\n");

        //write the crawl index
        Map<String, String> url_indexes = new HashMap<>();
        if (!FileUtils.fileExists(index_file, true)) {
            int url_index = 0;
            StringBuffer sb = new StringBuffer();
            for (String event_line : crawl_urls) {
                String[] tmp = event_line.split("\t");
                String url = tmp[tmp.length - 1];
                if (url_indexes.containsKey(url)) {
                    continue;
                }

                url_indexes.put(url, "file_" + url_index + ".html");
                sb.append(url).append("\t").append("file_" + url_index + ".html").append("\n");
                url_index++;
            }
            FileUtils.saveText(sb.toString(), index_file);
        } else {
            url_indexes = FileUtils.readIntoStringMap(index_file, "\t", false);
        }

        //crawl the URLs.
        String crawl_dir = output_dir + "/raw_files/";
        FileUtils.checkDir(crawl_dir);
        crawl_dir = crawl_dir + "/" + date;
        FileUtils.checkDir(crawl_dir);
        crawlURLs(url_indexes, thread_no, crawl_dir);
    }

    /**
     * For the crawled files from a specific GDelt events file clean the HTML tags.
     *
     * @param output_dir
     * @param date
     * @param year
     * @throws InterruptedException
     */
    public void boilerPipeCleanCrawledURLs(String output_dir, String date, int year) throws InterruptedException, IOException {
        //read first the index file containing the mapping of files to the respective folders
        String index_file = output_dir + "/file_index/index_file_" + date + ".csv";
        Map<String, String> crawl_index = FileUtils.readIntoStringMap(index_file, "\t", false);

        FileUtils.checkDir(output_dir + "/cleaned/");
        FileUtils.checkDir(output_dir + "/cleaned/" + year + "/");
        String out_file = output_dir + "/cleaned/" + year + "/" + date + "_cleaned_articles.csv";


        //in this case the cleaning of files has finished, hence, move to the next step.
        if (FileUtils.fileExists(out_file + ".gz", false)) {
            return;
        }
        StringBuffer sb = new StringBuffer();

        for (String url : crawl_index.keySet()) {
            String file = crawl_index.get(url);
            String file_name = output_dir + "/raw_files/" + date + "/" + file + ".gz";
            if (!FileUtils.fileExists(file_name, false)) {
                return;
            }
            try {
                String json_line = cleanANDConvertJSON(file_name, url, date);
                if (json_line.isEmpty()) {
                    continue;
                }
                sb.append(json_line);
                System.out.printf("Finished processing file %s\n", file_name);
            } catch (Exception e) {
                System.out.printf("Error at file %s with message %s\n", file, e.getMessage());
            }
            if (sb.length() > 10000) {
                FileUtils.saveText(sb.toString(), out_file, true);
                sb.delete(0, sb.length());
            }
        }
        FileUtils.saveText(sb.toString(), out_file, true);
        Process p = Runtime.getRuntime().exec("gzip " + out_file);
        p.waitFor();
    }


    /**
     * Index the crawled documents into SOLR.
     *
     * @param input_dir
     */
    public void indexIntoSOLR(String input_dir, int year, String date) throws IOException, SolrServerException {
        String cleaned_articles = input_dir + "/cleaned/" + year + "/" + date + "_cleaned_articles.csv.gz";
        SolrClient server = new HttpSolrClient("http://meco.l3s.uni-hannover.de:8983/solr/gdelt_all/");

        BufferedReader reader = FileUtils.getFileReader(cleaned_articles);
        List<SolrInputDocument> docs_tmp = new ArrayList<>();
        while (reader.ready()) {
            String doc_line = reader.readLine();
            JSONObject json_obj = new JSONObject(doc_line);
            SolrInputDocument doc_tmp = new SolrInputDocument();

            if (json_obj.getString("content").isEmpty()) {
                continue;
            }

            doc_tmp.addField("url", json_obj.getString("url"));
            doc_tmp.addField("date", json_obj.getString("date"));
            doc_tmp.addField("content", json_obj.getString("content"));
            doc_tmp.addField("title", json_obj.getString("title"));
            docs_tmp.add(doc_tmp);

            if (docs_tmp.size() % 10000 == 0) {
                commitDocsToServer(docs_tmp, server);
            }
        }
        commitDocsToServer(docs_tmp, server);
        System.out.printf("Finished adding %d documents into the Solr Index.\n", docs_tmp.size());
    }

    /**
     * Commit the documents into the Server, in case some document cannot be added fall back to adding them individually
     * so that we can isolate the one faulty document.
     *
     * @param docs_tmp
     * @param server
     */
    private static void commitDocsToServer(List<SolrInputDocument> docs_tmp, SolrClient server) {
        try {
            server.add(docs_tmp);
            server.commit();
            docs_tmp.clear();
        } catch (Exception e) {
            System.out.println("Error while adding documents " + e.getMessage());

            for (SolrInputDocument doc : docs_tmp) {
                try {
                    server.add(doc);
                    server.commit();
                } catch (Exception ex) {
                    System.out.println("Error while adding document " + doc.getField("url") + "\t" + ex.getMessage());
                }
            }
        }
        docs_tmp.clear();
    }


    /**
     * Run the GDelt crawler service.
     *
     * @param args
     * @throws InterruptedException
     * @throws IOException
     * @throws ParseException
     */
    public static void main(String[] args) throws InterruptedException, IOException, ParseException {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-output_dir")) {
                output_dir = args[++i];
            } else if (args[i].equals("-threads")) {
                thread_no = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-filter")) {
                crawl_filter = FileUtils.readIntoSet(args[++i], "\n", true);
            } else if (args[i].equals("-min_year")) {
                min_year = args[++i];
            } else if (args[i].equals("-min_month")) {
                min_month = args[++i];
            } else if (args[i].equals("-min_day")) {
                min_day = args[++i];
            }
        }

        Timer timer = new Timer();
        Calendar date = Calendar.getInstance();
        Calendar today = Calendar.getInstance();
        today.set(Calendar.HOUR_OF_DAY, 18);
        today.set(Calendar.MINUTE, 0);
        today.set(Calendar.SECOND, 0);

        // Schedule to run every Sunday in midnight
        timer.schedule(new GdeltCrawler(), date.getTime(), TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
    }

    /**
     * Converts to JSON the extracted content from the web page. The fields of the JSON object are:
     * url, date, doc_content, title.
     *
     * @param url
     * @param file_name
     * @return
     * @throws BoilerpipeProcessingException
     */
    private String cleanANDConvertJSON(String file_name, String url, String date) throws BoilerpipeProcessingException {
        String doc_content = FileUtils.readText(file_name);
        String text = ArticleExtractor.getInstance().getText(doc_content);
        String title = Jsoup.parse(doc_content).title();

        if (text.isEmpty()) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        sb.append("{\"url\":\"").append(StringEscapeUtils.escapeJson(url)).
                append("\",\"date\":\"").append(date).
                append("\",\"content\":\"").append(StringEscapeUtils.escapeJson(text)).
                append("\",\"title\":\"").append(StringEscapeUtils.escapeJson(title)).append("\"}\n");
        return sb.toString();
    }


    /**
     * Crawls a set of URLs as given by a URL index.
     *
     * @param crawl
     * @param threads
     * @param out_dir
     * @throws InterruptedException
     */
    private void crawlURLs(Map<String, String> crawl, int threads, String out_dir) throws InterruptedException {
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);

        crawl.keySet().parallelStream().forEach(url_to_index -> {
            Runnable r = () -> {
                String url_file = crawl.get(url_to_index);
                if (FileUtils.fileExists(out_dir + "/" + url_file + ".gz", false) || FileUtils.fileExists(out_dir + "/" + url_file, false)) {
                    return;
                }

                //check if the URL is not allowed to be crawled.
                if (url_file.lastIndexOf(".") != -1) {
                    String suffix = url_file.substring(url_file.lastIndexOf(".")).toLowerCase().trim();
                    if (crawl_filter.contains(suffix)) {
                        return;
                    }
                }

                try {
                    Process p = Runtime.getRuntime().exec("wget -O " + out_dir + "/" + url_file + " " + url_to_index);
                    p.waitFor(5, TimeUnit.SECONDS);
                    p.destroy();

                    p = Runtime.getRuntime().exec("gzip " + out_dir + "/" + url_file);
                    p.waitFor();
                    p.destroy();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Finished " + url_file);
            };
            threadPool.submit(r);
        });

        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        try {
            downloadEventData(min_year, min_month, min_day, output_dir);
            System.out.printf("Finished crawling GDelt events database for the date up to %s.\n", new DateTime().toDate());
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
            e.printStackTrace();
        }
    }
}
