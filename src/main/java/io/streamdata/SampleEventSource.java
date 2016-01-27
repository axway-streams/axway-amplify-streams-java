package io.streamdata;

// jackson imports
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

// zjsonpatch import
import com.flipkart.zjsonpatch.JsonPatch;

// jersey imports
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.sse.EventListener;
import org.glassfish.jersey.media.sse.EventSource;
import org.glassfish.jersey.media.sse.InboundEvent;
import org.glassfish.jersey.media.sse.SseFeature;

// logger imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// java web service client imports
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedHashMap;

// java IOException import
import java.io.IOException;

/**
 * This is a sample implementation of an EventSource (https://www.w3.org/TR/eventsource/) in Java to use Streamdata.io service.
 *
 *
 * It uses Jersey (https://jersey.java.net) as a Java EventSource implementation (https://jersey.java.net/documentation/latest/sse.html)
 * It uses zjsonpatch (https://github.com/flipkart-incubator/zjsonpatch) as a Java Json-Patch implementation.
 * zjsonpatch relies itself on jackson Java Json library (http://wiki.fasterxml.com/JacksonHome).
 */
public class SampleEventSource implements Runnable {
    // define a slf4j logger
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleEventSource.class);

    // The API URL string
    private final String url;

    // jackson objectMapper to parse Json content
    private final ObjectMapper jsonObjectMapper = new ObjectMapper();

    // local storage of the data
    private JsonNode data;

    /**
     * SampleEventSource constructor.
     * @param aUrl the API URL
     */
    SampleEventSource(String aUrl){
        url = aUrl;
    }

    @Override
    public void run() {

        try {
            // create a client using jersey Server-Sent event library
            Client client = ClientBuilder.newBuilder().register(SseFeature.class).build();

            // create the webTarget for the client (URL to stream)
            WebTarget target = client.target(url);

            // build EventSource object to handle Streamdata.io Server-Sent events for this target
            // Streamdata.io will send two types of events :
            // 'data' event : this is the first event received to provide a full set of data and initialize the data stream. the streamdata.io specific 'data' event will be triggered
            // when a fresh Json data set is pushed by Streamdata.io coming from the API.
            // 'patch' event : this is a patch event to apply to initial data set. The streamdata.io specific 'patch' event will be triggered when a fresh Json patch
            // is pushed by streamdata.io coming from the API. This patch has to be applied to the latest data set.
            new EventSource(target) {
                @Override
                public void onEvent(InboundEvent inboundEvent) {
                    // get event name
                    String eventName = inboundEvent.getName();
                    // get event data
                    String eventData = new String(inboundEvent.getRawData());

                    if ("data".equals(eventName)) {
                        // event name is "data": handle reception of a 'data' event
                        try {
                            // simply set local storage with this data set
                            data = jsonObjectMapper.readTree(eventData);
                            LOGGER.debug("Data received : {}\n\n", data.toString());
                        } catch (IOException e) {
                            LOGGER.error("Data received is not Json format: {}", eventData, e);
                            // closing stream in case of error
                            this.close();
                        }
                    } else if ("patch".equals(eventName)) {
                        // event name is "patch": handle reception of a 'patch' event
                        try {
                            // read the patch and set in in a local variable
                            JsonNode patchNode = jsonObjectMapper.readTree(eventData);
                            LOGGER.debug("Patch received : {}", patchNode.toString());
                            // apply patch to the local storage
                            LOGGER.debug("Applying patch ...");
                            data = JsonPatch.apply(patchNode, data);
                            // data set is then updated.
                            LOGGER.debug("Data updated: {}\n\n", data.toString());
                        } catch (IOException e) {
                            LOGGER.error("Patch received is not Json format: {}.", eventData, e);
                            // closing stream in case of error
                            this.close();
                        }
                    } else if ("error".equals(eventName)) {
                        LOGGER.error("An error occured: {}.", eventData);
                        // closing stream in case of error
                        this.close();
                    } else {
                        // add code here for any other event type
                        // streamdata.io
                        LOGGER.debug("Unhandled event received: {}\n\n", eventData);
                    }
                }
            };
        } catch (Exception e) {
            LOGGER.error("An Error occured.", e);
            System.exit(0);
        }

    }
}
