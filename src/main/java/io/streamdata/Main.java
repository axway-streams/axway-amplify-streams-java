package io.streamdata;

public class Main {
    /**
     * simple main class to run a thread connecting to a sample API using Streamdata.io service.
     *
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {


        // the sample API
        String url = "https://streamdata.motwin.net/http://stockmarket.streamdata.io/prices?X-Sd-Token=[YOUR TOKEN HERE]";

        // Start the event source in a Thread.
        Thread t = new Thread(new SampleEventSource(url));
        t.setDaemon(true);
        t.start();

        // let the sample app run for a few seconds.
        Thread.sleep(30000);

        // then exit.
        System.exit(0);
    }
}

