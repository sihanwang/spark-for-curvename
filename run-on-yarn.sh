#!/bin/sh

# Run an example spark job for generating Curve names for loadset with ID 34052 in Alpha environment

spark-submit --class curvename.StageCurveName --master yarn-cluster --deploy-mode cluster target/MetaDataAutosuggest-0.0.1-SNAPSHOT-jar-with-dependencies.jar 34052 /config/alpha-dtc-default/config.properties    