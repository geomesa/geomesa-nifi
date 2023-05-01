# GeoMesa NiFi

See the official GeoMesa [documentation](https://www.geomesa.org/documentation/stable/user/nifi/index.html) for details.

## Development

Requirements:

* Java JDK 8
* Maven 3.6 or later

GeoMesa NiFi builds with Maven:

    mvn clean install

After building, NiFi can be run for local testing through the provided script:

    ./build/run-nifi.sh <nar-to-mount>

The script requires the name of a back-end NAR to mount, such as `accumulo21` or `kafka`. Run the script
without any arguments to see the available NARs. Once started, NiFi can be accessed at
`http://localhost:8080/nifi`. The default credentials are `nifi`/`nifipassword`. There is a small flow
already bundled, which will attempt to ingest a few thousand rows of GDELT data. The flow does not include
a data store service, which must be configured depending on the NAR being mounted and the local environment.
