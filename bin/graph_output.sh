#!/bin/sh
die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "Please provide output bucket name as an argument"
: ${MRLOADER_S3_ROOT:?"Please set MRLOADER_S3_ROOT to valid S3 bucket. e.g s3://my_loader_bucket"}

TEMP_DIR=./temp/$1/out

if [ -d "$TEMP_DIR" ]; then
   rm -R $TEMP_DIR
else
   mkdir -p $TEMP_DIR
fi

aws s3 cp $MRLOADER_S3_ROOT/output/$1/ $TEMP_DIR --recursive > /dev/null
cat $TEMP_DIR/part* > $TEMP_DIR/merge.txt
sort -n $TEMP_DIR/merge.txt > $1.sorted.txt

cat << EOF | gnuplot > $1.png
set terminal png
set autoscale
unset log
unset label
set xtic auto
set ytic auto
set title "Batch Latency"
set ylabel "# of batches"
set xlabel "latency (ms)"
set logscale y
set logscale x
plot "$1.sorted.txt" using 1:2 title "$1"
EOF

aws s3 cp $1.png $MRLOADER_S3_ROOT/graphs/ 

