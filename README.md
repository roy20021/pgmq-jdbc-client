# pgmq-jdbc-client
Java JDBC Client for the [PGMQ](https://github.com/pgmq/pgmq) PostgreSQL Extension

Works seamlessly with PGMQ's semantics: persistent queueing, acknowledgement, visibility, retries as supported by the extension.
This means that if you already have a PostgreSQL + PGMQ setup, you can plug in this library to your Java application and start producing and consuming messages via JDBC.

## Features

- Lightweight/Minimal dependencies: the aim is to keep it light, framework-agnostic, compatible with plain Java applications (no Spring, no heavy container).
- Retry logic

## Getting Started

Here's how you get started using the library in your project.

If you are using Maven, in your pom.xml you might declare:

    <dependency>
      <groupId>org.andreaesposito</groupId>
      <artifactId>pgmq-jdbc-client</artifactId>
      <version>1.0.0</version>
    </dependency>

and add the GitHub Repo (requires authentication):

    <repository>
       <id>github</id>
       <url>https://maven.pkg.github.com/roy20021/pgmq-jdbc-client</url>
     </repository>

Then, instantiate the client with the _current_ connection and use it:
````java
Connection connection = ...;

PgmqJdbcClient client = new PgmqJdbcClient(connection);

// Send
Json msg = new Json("{\"hello\":\"world\"}");
List<Long> ids = client.send("myQueue", msg);

// Read
List<MessageRecord> read = client.read("myQueue", 10, 1, null);
````
