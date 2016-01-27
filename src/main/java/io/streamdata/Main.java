package io.streamdata;

import java.net.URI;
import java.net.URISyntaxException;

public class Main {
    /**
     * simple main class to run a thread connecting to a sample API using Streamdata.io service.
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {


        // the sample API
        String apiUrl = "http://stockmarket.streamdata.io/prices";

        String token = "[YOUR TOKEN HERE]";

        String url = buildStreamdataUrl(apiUrl, token);

        // Start the event source in a Thread.
        Thread t = new Thread(new SampleEventSource(url));
        t.setDaemon(true);
        t.start();

        // let the sample app run for a few seconds.
        Thread.sleep(30000);

        // then exit.
        System.exit(0);
    }


    private static String buildStreamdataUrl(String anApiUrl,String aToken) throws URISyntaxException {
        URI uri = new URI(anApiUrl);
        String aqueryParamSeparator = (uri.getQuery() == null || uri.getQuery().isEmpty() )?"?":"&";
        return String.format("https://streamdata.motwin.net/%s%sX-Sd-Token=%s", anApiUrl,aqueryParamSeparator, aToken);
    }
}

