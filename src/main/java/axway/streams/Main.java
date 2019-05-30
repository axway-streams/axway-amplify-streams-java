/*
 * Copyright 2016 Axway Inc.
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

package axway.streams;

import com.google.common.base.Preconditions;
import jersey.repackaged.com.google.common.collect.Maps;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.stream.Collectors;

public class Main {
    /**
     * simple main class to run a thread connecting to a sample API using Axway AMPLIFY Streams service.
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {


        // the sample API
        String apiUrl = "http://stockmarket.streamdata.io/v2/prices";

        String token = "ZjhjMTQwZGYtYmVjZi00OGVjLWE2MzctYjgyMGE5NDFiMTBk";

        // specific header map associated with the request
        Map<String,String> headers = Maps.newHashMap();

        // add this header if you wish to stream Json rather than Json-patch
        // NOTE: no 'patch' event will be emitted.

        // headers.put("Accept", "application/json");

        String url = buildAxwayStreamsUrl(apiUrl, token, headers);


        // Start the event source as a Thread.
        SampleEventSource eventSource = new SampleEventSource(url);
        eventSource.setDaemon(true);
        eventSource.start();

        // let the sample app run for a few seconds.
        Thread.sleep(30000);

        // Close the eventSource
        eventSource.close();

        // then exit.
        System.exit(0);

    }


    private static String buildAxwayStreamsUrl(String anApiUrl,String aToken,Map<String,String> aHeaders) throws URISyntaxException {
        Preconditions.checkNotNull(anApiUrl,"anApiUrl cannot be null");
        Preconditions.checkNotNull(aToken,"aToken cannot be null");
        Preconditions.checkNotNull(aHeaders,"aHeaders cannot be null");
        URI uri = new URI(anApiUrl);
        String aqueryParamSeparator = (uri.getQuery() == null || uri.getQuery().isEmpty() )?"?":"&";

        // proxify and add auth token to API url
        String url = String.format("https://streamdata.motwin.net/%s%sX-Sd-Token=%s", anApiUrl,aqueryParamSeparator, aToken);

        // add headers if any to API url
        String additionalHeaders = aHeaders.keySet().stream().map(key -> "X-Sd-Header" + "=" +  key + ":" + aHeaders.get(key)).collect(Collectors.joining("&"));

        return url + ((!additionalHeaders.isEmpty())?"&":"") + additionalHeaders;
    }
}

