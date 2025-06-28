#!/bin/bash

echo "Downloading required JAR files for Excel support..."

# Apache POI and related dependencies
curl -O https://repo1.maven.org/maven2/org/apache/poi/poi/5.2.3/poi-5.2.3.jar
curl -O https://repo1.maven.org/maven2/org/apache/poi/poi-ooxml/5.2.3/poi-ooxml-5.2.3.jar
curl -O https://repo1.maven.org/maven2/org/apache/poi/ooxml-schemas/1.4/ooxml-schemas-1.4.jar
curl -O https://repo1.maven.org/maven2/org/apache/xmlbeans/xmlbeans/3.1.0/xmlbeans-3.1.0.jar
curl -O https://repo1.maven.org/maven2/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar
curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.21/commons-compress-1.21.jar

# spark-excel package
curl -O https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.12/0.13.7/spark-excel_2.12-0.13.7.jar

echo "All JAR files downloaded successfully into /opt/glue/libs"
