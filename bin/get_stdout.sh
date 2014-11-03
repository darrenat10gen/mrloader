#!/bin/sh
die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "Please provide a single EMR JobId as an argument"
: ${MRLOADER_S3_ROOT:?"Please set MRLOADER_S3_ROOT to valid S3 bucket. e.g s3://my_loader_bucket"}

TEMP_DIR=./temp/$1/stdout

if [ -d "$TEMP_DIR" ]; then
   rm -R $TEMP_DIR
fi
mkdir -p $TEMP_DIR

aws s3 cp $MRLOADER_S3_ROOT/logs/$1/task-attempts/ $TEMP_DIR --recursive > /dev/null
find $TEMP_DIR -name stdout -size 1 | xargs cat > $1.stdout

