#!/bin/sh
die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "Please provide a single EMR JobId as an argument"
: ${MRLOADER_S3_ROOT:?"Please set MRLOADER_S3_ROOT to valid S3 bucket. e.g s3://my_loader_bucket"}

TEMP_DIR=./temp/$1/out

if [ -d "$TEMP_DIR" ]; then
   rm -R $TEMP_DIR
fi
mkdir -p $TEMP_DIR

aws s3 cp $MRLOADER_S3_ROOT/output/$1/ $TEMP_DIR --recursive > /dev/null
cat $TEMP_DIR/part* > $TEMP_DIR/merge.txt
sort -n $TEMP_DIR/merge.txt > $1.sorted.txt
export col2='$2'

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

# function for cumulative total on y axis
a=0
cummulative_sum(x)=(a=a+x,a)

# use stats to find, mark and label 95th percentile
stats "$1.sorted.txt" 
mark = (STATS_sum_y)*0.95
set arrow 1 from graph 0.11,first mark to graph 1,first mark ls 1 nohead front
set label "95th %" at 1.1,mark

# Plot latency vs cumulative total on log scale
set logscale x
plot "$1.sorted.txt" using 1:(cummulative_sum($col2)) title "$1"

EOF

aws s3 cp $1.png $MRLOADER_S3_ROOT/graphs/ 


