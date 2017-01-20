# luwak-server

A currently barebones Dropwizard wrapper around Luwak.

## Running

**In Intellij:**
- Locate uk.co.flax.luwak.server.LuwakServer
- Right-click and click Run (or Debug)
- You should see something like this in your console:

`org.eclipse.jetty.server.Server: Started @3701ms`

- If you visit http://localhost:8081/ in a browser you should see
 Dropwizard's admin interface.
- By default, all resources are exposed on port 8080

**From the command line**

In `luwak-server`, Build the jar with `mvn package`, then run

    java -jar target/luwak-server-{version number here}-SNAPSHOT.jar
