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

**UI**

There is a basic UI accessible at `localhost:8080/ui`

**To add queries**

Create a query input file in the following JSON format:
```
[
{ "id" : "queryid", "query" : "your query here" },
{ "id" : "query2", "query" : "another query" }
]
```
There's an example [here](src/test/resources/queries.json)

Then run ```./load-queries <query file>```

**To match a document against the service**

./match "This is the contents of my document"

## Deploying to Heroku
Roughly following the instructions at https://devcenter.heroku.com/articles/getting-started-with-java#introduction
* Install the Heroku CLI
* If you have a public SSH key, you can upload it to Heroku
  to make your life easier: `heroku keys:add ~/.ssh/id_rsa.pub`
* Create a Heroku app: `heroku create`
* Push your current branch to heroku: `git push heroku HEAD:master`
* Once that succeeds, start the app via: `heroku ps:scale web=1`
* Run `heroku open` to get the URL of the running application. You should be able to
  to post to the API by appending the correct path, e.g.
  `POST https://my-app-name.herokuapp.com/update`