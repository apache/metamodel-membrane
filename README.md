## Apache MetaModel Membrane

Data Federation as a RESTful service. A subproject of [Apache MetaModel](http://metamodel.apache.org).

<div>
<img src="http://metamodel.apache.org/img/logo.png" style="float: right; margin-left: 20px;" alt="MetaModel logo" />
</div>

### Building and running

Make sure you have [Apache Maven](http://maven.apache.org/), then build by invoking:

```
mvn clean install
```

After building the Java archives and executables, you can use [Docker](https://www.docker.com/) and [Docker compose](https://docs.docker.com/compose/) to run Membrane easily, like this:

```
docker-compose up --build
```

Now Membrane should be running on port `8080` of your Docker host. Typically that's either http://localhost:8080 (if you have a native Docker install) or http://192.168.99.100:8080 (if you use Docker toolbox).

### Mailing lists

Membrane uses the same development infrastructure as the main Apache MetaModel project:

 * Developer list:  dev@metamodel.apache.org
 * User list:  user@metamodel.apache.org
 * Commits list:    commits@metamodel.apache.org

### Contributing

Please see [CONTRIBUTE.md from Apache MetaModel](https://github.com/apache/metamodel/blob/master/CONTRIBUTE.md) which also apply to the Membrane contribution guidelines.
