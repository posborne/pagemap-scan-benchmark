#!/usr/bin/bash

set -e

THREADS="${THREADS:-4}"
PROCESSES="${PROCESSES:-16}"
ITERATIONS="${ITERATIONS:-100}"

run_test() {
    size="$1"
    dirty_pct="$2"
    fname="res-i${ITERATIONS}-p${PROCESSES}-t${THREADS}-${size}-${dirty_pct}.json"

    echo "${fname}"

    seq "${PROCESSES}" | parallel -n0 -j "${PROCESSES}" \
        ./target/release/pagemap-scan-benchmark \
        -s "${size}" \
        -d "${dirty_pct}" \
        -i "${ITERATIONS}" \
        -t "${THREADS}" \
        -p "${PROCESSES}" \
        --json \
        | jq -s> "${fname}"
}

run_test_array() {
    size="$1"
    run_test $size 1
    run_test $size .8
    run_test $size .5
    run_test $size .2
    run_test $size .1
    run_test $size .05
    run_test $size .01
    run_test $size .001
    run_test $size 0
}

cargo build --release
run_test_array 8K
run_test_array 16K
run_test_array 32K
run_test_array 64K
run_test_array 128K
run_test_array 256K
run_test_array 512K
run_test_array 1M
run_test_array 4M
run_test_array 8M
run_test_array 16M
