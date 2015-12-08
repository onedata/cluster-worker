REPO	        ?= cluster-worker

BASE_DIR	     = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR	        ?= $(BASE_DIR)/rebar
OVERLAY_VARS    ?=

.PHONY: deps test package

all: test_rel

##
## Rebar targets
##

recompile:
	./rebar compile skip_deps=true

compile:
	./rebar compile

deps:
	./rebar get-deps

generate: deps compile
	./rebar generate $(OVERLAY_VARS)

clean: relclean
	./rebar clean

distclean:
	./rebar delete-deps

##
## Release targets
##

rel: generate

test_rel: generate ccm_rel appmock_rel

ccm_rel:
	make -C op_ccm/ rel

appmock_rel:
	make -C appmock/ rel

relclean:
	rm -rf rel/test_cluster
	rm -rf rel/op_worker
	rm -rf appmock/rel/appmock
	rm -rf op_ccm/rel/op_ccm

##
## Testing targets
##

eunit:
	./rebar eunit skip_deps=true suites=${SUITES}
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

coverage:
	$(BASE_DIR)/bamboos/docker/coverage.escript $(BASE_DIR)

##
## Dialyzer targets local
##

PLT ?= .dialyzer.plt

# Builds dialyzer's Persistent Lookup Table file.
.PHONY: plt
plt:
	dialyzer --check_plt --plt ${PLT}; \
	if [ $$? != 0 ]; then \
	    dialyzer --build_plt --output_plt ${PLT} --apps kernel stdlib sasl erts \
		ssl tools runtime_tools crypto inets xmerl snmp public_key eunit \
		mnesia edoc common_test test_server syntax_tools compiler ./deps/*/ebin; \
	fi; exit 0


# Dialyzes the project.
dialyzer: plt
	dialyzer ./ebin --plt ${PLT} -Werror_handling -Wrace_conditions --fullpath
