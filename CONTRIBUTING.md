Contributing to GeoMesa-NiFi
============================

Project Description
-------------------

GeoMesa-NiFi is a bundle of NiFi processors to allow NiFi to leverage 
GeoMesa's converter library in order to ingest data into an arbitrary 
GeoTools datastore or convert data to geoavro.

- http://www.geomesa.org/

Eclipse Contributor Agreement
-----------------------------

Before your contribution can be accepted by the project, you need to create an Eclipse Foundation 
account and electronically sign the Eclipse Contributor Agreement (ECA).

- http://www.eclipse.org/legal/ECA.php 

Developer Resources
-------------------

GeoMesa-NiFi code is hosted on GitHub, and is part of the GeoMesa community modules:

* https://github.com/geomesa/geomesa-nifi
* https://github.com/geomesa

Issue Tracking
--------------

GeoMesa-NiFi uses GitHub to track ongoing development and issues:

* https://github.com/geomesa/geomesa-nifi/issues

Building
--------

GeoMesa-NiFi requires Maven 3.2.2 or later to build:

```
GeoMesa-NiFi> mvn clean install
```

Contributing
------------

GeoMesa-NiFi uses git pull requests for contributions. To create a pull request, follow these steps:

* Fork the GeoMesa-NiFi project on GitHub - go to https://github.com/locationtech/GeoMesa-NiFi and click 'Fork'.
* Create a branch on your forked project that contains your work. See 'Coding Standards', below.
* Use GitHub to open a pull request against the GeoMesa-NiFi repository - from your branch on
  GitHub, click 'New Pull Request'.
* Respond to comments on your pull request as they are made.
* When ready, your pull request will be merged by an official GeoMesa contributor.

Coding Standards
----------------

* An initial pull request should consist of a single commit, rebased on top of the current master branch.
  * Additional commits can be added in response to code review comments.
* The commit must be signed-off, which indicates that you are taking responsibility for all code contained
  in the commit. This can be done with the `git commit -s` flag.
* Code must be reasonably formatted. Scala does not conform well to automatic formatting, but in general
  GeoMesa-NiFi tries to adhere to the official Scala style guide: http://docs.scala-lang.org/style/
* Code must contain an appropriate license header. GeoMesa-NiFi is licensed under Apache 2.  When a file is created, the contributor should indicate their copyright in the header.
  * GeoMesa-NiFi uses the License Maven Plugin (http://code.mycila.com/license-maven-plugin/) to help manage copyright headers.  In order to handle various headers, files can be put in the `build/` directory and referenced in a `validHeaders` block in the root pom.xml.
* Code should include unit tests when appropriate.

Contact
-------

Contact the GeoMesa developers via the developers mailing list:

* https://www.locationtech.org/mailman/listinfo/GeoMesa-dev

For user information, use the users mailing list:

* https://www.locationtech.org/mailman/listinfo/GeoMesa-users
