/*
 * Copyright 2016 Streamdata.io
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package io.streamdata;

// jackson imports
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

// zjsonpatch import
import com.flipkart.zjsonpatch.JsonPatch;

// jersey imports
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
public class SampleEventSource extends Thread {
    // define a slf4j logger
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleEventSource.class);

    // The API URL string
    private String url;

    // jackson objectMapper to parse Json content
    private final ObjectMapper jsonObjectMapper = new ObjectMapper();

    // local storage of the data
    private JsonNode data;

    // create a client using jersey Server-Sent event library
    private final  Client client = ClientBuilder.newBuilder().register(SseFeature.class).build();

    // create the webTarget for the client (URL to stream)
    private WebTarget target;

    private EventSource eventSource;

    /**
     * SampleEventSource constructor.
     * @param aUrl the API URL
     */
    SampleEventSource(String aUrl){
        url = aUrl;
        target = client.target(url);
    }

    private SampleEventSource() {

    }

    public void close(){
        LOGGER.debug("Closing EventSource ...");
        if (eventSource != null) {
            eventSource.close();
        }
        LOGGER.debug("EventSource closed.");
    }

    @Override
    public void run() {

        try {


            // build EventSource object to handle Streamdata.io Server-Sent events for this target
            // Streamdata.io will send two types of events :
            // 'data' event : this is the first event received to provide a full set of data and initialize the data stream. the streamdata.io specific 'data' event will be triggered
            // when a fresh Json data set is pushed by Streamdata.io coming from the API.
            // 'patch' event : this is a patch event to apply to initial data set. The streamdata.io specific 'patch' event will be triggered when a fresh Json patch
            // is pushed by streamdata.io coming from the API. This patch has to be applied to the latest data set.
            LOGGER.debug("Starting EventSource ...");
            eventSource = new EventSource(target) {
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
            close();
            System.exit(0);
        }

    }
}
