#!/usr/bin/env bash

sed -i -e 's|<scala.version>2.11</scala.version>|<scala.version>2.12</scala.version>|g' pom.xml
sed -i -e 's|<scalalib.version>2.11.4</scalalib.version>|<scalalib.version>2.12.1</scalalib.version>|g' pom.xml
rm -f pom.xml-e