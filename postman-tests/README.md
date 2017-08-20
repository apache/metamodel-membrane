# Membrane Postman tests

This folder has a set of tests and examples for use with the [Postman](https://www.getpostman.com/) tool.

## Environment files

The environments folder contains a set of reusable environment configs. Creating your own custom environment file should also be trivial since we only currently have one environment variable: `baseUrl`. 

## Prerequisites

The tests are designed to work with the default `docker-compose.yml` file in the root of the membrane project. This compose file has some databases and such which are used as part of the tests.

## Running from command line

If you have Postman's CLI tool `newman` installed, simply go like this:

```
newman run -e environments/docker-toolbox.postman_environment.json Membrane.postman_collection.json
```
(using the `docker-toolbox` environment)
