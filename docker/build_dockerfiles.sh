#!/bin/bash


DOCKERUSER="lkschu"




find . -maxdepth 1 -mindepth 1 -type d | while read d; do
    DOCKERDIR="$(basename "$d")"
    DOCKERIMAGE="$DOCKERUSER/$DOCKERDIR"
    echo "Building \"$DOCKERDIR\":"

    time docker build --no-cache --tag "$DOCKERIMAGE" "$d" 1>"$DOCKERDIR"-build.log 2>&1
    time docker push "$DOCKERIMAGE" 1>"$DOCKERDIR"-push.log 2>&1

    echo ""
    echo ""
done
