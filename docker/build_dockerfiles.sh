#!/bin/bash


DOCKERUSER="lkschu"




find . -maxdepth 1 -mindepth 1 -type d | while read d; do
    DOCKERDIR="$(basename "$d")"
    DOCKERIMAGE="$DOCKERUSER/$DOCKERDIR"
    echo "Building \"$DOCKERDIR\":"

    if docker inspect "$DOCKERIMAGE" 1> /dev/null 2>&1; then
        echo "Skipping $DOCKERIMAGE"
    else
        echo "Not skipping $DOCKERIMAGE"
        time docker build --no-cache --tag "$DOCKERIMAGE" "$d" 1>"$DOCKERDIR"-build.log 2>&1
        time docker push "$DOCKERIMAGE" 1>"$DOCKERDIR"-push.log 2>&1
    fi
    echo ""
    echo ""


done
