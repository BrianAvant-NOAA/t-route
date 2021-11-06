#!/bin/bash

if [[ $COMPILE -eq 1 ]]; then
    ## Compile t-route module ##
    cd /t-route/src/python_routing_v02 \
        && bash compiler.sh
fi

exec "$@"


