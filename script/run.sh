#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS=private/credentials/prominence-0e0ffcfbf903.json && \
    mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=com.mrgris.prominence.FullPipeline \
	-Dexec.args="--project=prominence-163319 \
          --gcpTempLocation=gs://mrgris-promviz/tmp/ \
          --stagingLocation=gs://mrgris-promviz/staging/ \
          --runner=DataflowRunner \
          --numWorkers=10 \
    "
