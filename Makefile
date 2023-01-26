REPO	        ?= cluster-worker

BASE_DIR	     = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR	        ?= $(BASE_DIR)/rebar3
OVERLAY_VARS    ?= --overlay_vars=rel/vars.config
LIB_DIR          = _build/default/lib
REL_DIR          = _build/default/rel

GIT_URL := $(shell git config --get remote.origin.url | sed -e 's/\(\/[^/]*\)$$//g')
GIT_URL := $(shell if [ "${GIT_URL}" = "file:/" ]; then echo 'ssh://git@git.onedata.org:7999/vfs'; else echo ${GIT_URL}; fi)
ONEDATA_GIT_URL := $(shell if [ "${ONEDATA_GIT_URL}" = "" ]; then echo ${GIT_URL}; else echo ${ONEDATA_GIT_URL}; fi)
export ONEDATA_GIT_URL

.PHONY: upgrade test package

all: test_rel

##
## Rebar targets
##

compile:
	$(REBAR) compile

upgrade:
	$(REBAR) upgrade

generate: compile
	$(REBAR) release $(OVERLAY_VARS)

clean: relclean
	$(REBAR) clean

distclean:
	$(REBAR) clean --all

##
## Submodules
##

submodules:
	git submodule sync --recursive ${submodule}
	git submodule update --init --recursive ${submodule}


##
## Release targets
##

rel: generate

test_rel: generate cm_rel

cm_rel:
	mkdir -p cluster_manager/bamboos/gen_dev
	make -C $(LIB_DIR)/cluster_manager/ submodules
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
	$(REBAR) do eunit skip_deps=true --suite=${SUITES}
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

eunit-with-cover:
	$(REBAR) do eunit skip_deps=true --suite=${SUITES}, cover
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

coverage:
	$(BASE_DIR)/bamboos/docker/coverage.escript $(BASE_DIR) $(on_bamboo)

##
## Dialyzer targets local
##

# Dialyzes the project.
dialyzer:
	@./bamboos/scripts/run-with-surefire-report.py \
		--test-name Dialyze \
		--report-path test/dialyzer_results/TEST-dialyzer.xml \
		$(REBAR) dialyzer

codetag-tracker:
	@./bamboos/scripts/run-with-surefire-report.py \
		--test-name CodetagTracker \
		--report-path test/codetag_tracker_results/TEST-codetag_tracker.xml \
		./bamboos/scripts/codetag-tracker.sh --branch=${BRANCH} \
		--excluded-dirs=node_package,locks,codetag_tracker_results
