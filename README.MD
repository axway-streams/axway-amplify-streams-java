# Simple Java app using streamdata.io

## Step by step setup

1. Create an free account on streamdata.io https://portal.streamdata.io/#/register to get an App token.

2. Clone project, edit Main.java and replace ```[YOUR TOKEN HERE]``` with your App token.

3. Make sure you have Java 8+ installed

4. Make sure you have maven 3.0+ installed

5. Build project with maven:

  ```
  mvn clean install
  ```

6. Run sample from a terminal:

  ```
  java -jar target/streamdataio-java-0.0.1-SNAPSHOT.jar
  ```  

You should see data and patches pushed in your application and displayed on your terminal.

you can use the provided demo example API which simulates updating stocks prices from a financial market:
'http://stockmarket.streamdata.io/prices'

Feel free to test it with any REST/Json API of your choice.
