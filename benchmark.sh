#!/usr/bin/bash

run_test() {
    size="$1"
    keep_resident="$2"
    dirty_pct="$3"
    parallel=${4:-32}
    iterations=${5:-10}
    fname="res-j${parallel}-${size}-${keep_resident}-${dirty_pct}.json"

    echo "${fname}"
    seq 32 | parallel -n0 -j 16 ./target/release/pagemap-scan-benchmark -s "${size}" -r "${keep_resident}" -d "${dirty_pct}" -i "${iterations}" -j | jq -s> "${fname}"
}

run_test_array() {
    size="$1"
    keep_resident="$2"
    run_test $size $keep_resident .5
    run_test $size $keep_resident .2
    run_test $size $keep_resident .1
    run_test $size $keep_resident .05
    run_test $size $keep_resident .01
    run_test $size $keep_resident .001
}

run_test_array 1G 128M
run_test_array 1M 128K
run_test_array 1M 256K
run_test_array 32M 4M
run_test_array 64K 8K
