REPO	        ?= cluster-worker

BASE_DIR	     = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR	        ?= $(BASE_DIR)/rebar3
OVERLAY_VARS    ?= --overlay_vars=rel/vars.config
LIB_DIR          = _build/default/lib
REL_DIR          = _build/default/rel

GIT_URL := $(shell git config --get remote.origin.url | sed -e 's/\(\/[^/]*\)$$//g')
GIT_URL := $(shell if [ "${GIT_URL}" = "file:/" ]; then echo 'ssh://git@git.plgrid.pl:7999/vfs'; else echo ${GIT_URL}; fi)
ONEDATA_GIT_URL := $(shell if [ "${ONEDATA_GIT_URL}" = "" ]; then echo ${GIT_URL}; else echo ${ONEDATA_GIT_URL}; fi)
export ONEDATA_GIT_URL

.PHONY: upgrade test package

all: test_rel

##
## Rebar targets
##

priv/sync_gateway:
	mkdir -p vendor/sync_gateway
	curl https://raw.githubusercontent.com/couchbase/sync_gateway/1.3/bootstrap.sh > vendor/sync_gateway/bootstrap.sh
	cd vendor/sync_gateway && sh bootstrap.sh && ./build.sh
	find vendor/sync_gateway -name shallow -type l -exec test ! -e '{}' ';' -delete
	cp vendor/sync_gateway/godeps/bin/sync_gateway priv/
	rm -rf vendor

compile: priv/sync_gateway
	$(REBAR) compile

upgrade: priv/sync_gateway
	$(REBAR) upgrade

generate: compile priv/sync_gateway
	$(REBAR) release $(OVERLAY_VARS)

clean: relclean
	$(REBAR) clean

distclean:
	$(REBAR) clean --all

##
## Release targets
##

rel: generate

test_rel: generate cm_rel

cm_rel:
	mkdir -p cluster_manager/bamboos/gen_dev
	cp -rf $(LIB_DIR)/cluster_manager/bamboos/gen_dev cluster_manager/bamboos
	printf "\n{base_dir, \"$(BASE_DIR)/cluster_manager/_build\"}." >> $(LIB_DIR)/cluster_manager/rebar.config
	make -C $(LIB_DIR)/cluster_manager/ rel
	sed -i "s@{base_dir, \"$(BASE_DIR)/cluster_manager/_build\"}\.@@" $(LIB_DIR)/cluster_manager/rebar.config

relclean:
	rm -rf $(REL_DIR)/test_cluster
	rm -rf $(REL_DIR)/cluster_worker
	rm -rf cluster_manager/$(REL_DIR)/cluster_manager

##
## Testing targets
##

eunit:
	$(REBAR) do eunit skip_deps=true suites=${SUITES}, cover
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

coverage:
	$(BASE_DIR)/bamboos/docker/coverage.escript $(BASE_DIR)

##
## Dialyzer targets local
##

# Dialyzes the project.
dialyzer:
	$(REBAR) dialyzer
