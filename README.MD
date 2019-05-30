# Simple Java app using Axway AMPLIFY Streams

This is a sample implementation of an EventSource (https://www.w3.org/TR/eventsource/) in Java. It uses the **Axway AMPLIFY Streams** service from a JVM. It relies on Jersey (https://jersey.java.net) as a Java EventSource implementation (https://jersey.java.net/documentation/latest/sse.html). It uses *zjsonpatch* (https://github.com/flipkart-incubator/zjsonpatch) as a Java Json-Patch implementation. *zjsonpatch* relies itself on Jackson Java Json library (http://wiki.fasterxml.com/JacksonHome).

## Step by step setup

1. Create an free account on Axway AMPLIFY Streams https://portal.streamdata.io/#/register to get an App token.

2. Clone project, edit Main.java and replace ```[YOUR TOKEN HERE]``` with your App token.

3. Make sure you have Java 8+ installed

4. Make sure you have maven 3.0+ installed

5. Build project with maven:

  ```bash
  mvn clean install
  ```

6. Run sample from a terminal:

  ```bash
  java -jar target/axway-streams-java-0.0.1-SNAPSHOT.jar
  ```  

You should see data and patches pushed in your application and displayed on your terminal.

You can use the provided demo example API that simulates updating stocks prices from a financial market: 'http://stockmarket.streamdata.io/v2/prices'

Feel free to test it with any REST/Json API of your choice.

## Contributing

Please read [Contributing.md](https://github.com/axway-amplify-streams/Common/blob/master/Contributing.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Team

![alt text][Axwaylogo] Axway Team

[Axwaylogo]: https://github.com/axway-amplify-streams/Common/blob/master/img/AxwayLogoSmall.png  "Axway logo"

## License

[Apache License 2.0](https://github.com/axway-amplify-streams/Common/blob/master/LICENSE)
