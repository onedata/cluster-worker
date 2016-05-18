#!/usr/bin/env bash

#####################################################################
# @author Tomasz Lichon, Michal Wrzeszcz
# @copyright (C): 2015 ACK CYFRONET AGH
# This software is released under the MIT license
# cited in 'LICENSE.txt'.
#####################################################################
# usage:
# ./erl_compile.sh [MODULE]...
#
# This script compiles modules selected as arguments, and replaces them in
# release: rel/cluster_worker.
#####################################################################

set -e

# for each given module, find its location, compile and copy to releases
for module in $@; do
    for dir in src test; do
        find ${dir} -type f -name ${module}.erl -exec erlc +debug_info `ls deps | awk '{ print "-I deps/" $1}'` -I include -I deps {} \;
        find ${dir} -type f -name ${module}.erl -exec cp {} .eunit/ \;
    done
    for rel_dir in `ls rel/cluster_worker/lib/ | grep cluster_worker`; do
        cp *.beam rel/cluster_worker/lib/${rel_dir}/ebin/
    done
    cp *.beam ebin/
done
rm *.beam
