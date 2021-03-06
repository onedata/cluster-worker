#!/usr/bin/env bash

#####################################################################
# @author Tomasz Lichon, Michal Wrzeszcz
# @copyright (C) 2015 ACK CYFRONET AGH
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
        find ${dir} -type f -name ${module}.erl -exec erlc +debug_info `ls _build/default/lib | awk '{ print "-I _build/default/lib/" $1}'` -I include -I _build/default/lib {} \;
    done
    for rel_dir in `ls _build/default/rel/cluster_worker/lib/ | grep cluster_worker`; do
        cp *.beam _build/default/rel/cluster_worker/lib/${rel_dir}/ebin/
    done
    cp *.beam _build/default/lib/cluster_worker/ebin/
done
rm *.beam
