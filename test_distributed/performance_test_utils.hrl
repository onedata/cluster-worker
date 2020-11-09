%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header defines common macros used in performance tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(PERFORMANCE_TEST_UTILS_HRL).
-define(PERFORMANCE_TEST_UTILS_HRL, 1).

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

-define(PERF_PARAM(Name, Value, Unit, Description), [
    {name, Name},
    {value, Value},
    {description, Description},
    {unit, Unit}
]).
-define(PERF_CFG(Name, Params), {config, [
    {name, Name},
    {description, atom_to_list(Name)},
    {parameters, Params}
]}).

-define(begin_measurement(Name),
    put(Name, os:timestamp()) % @TODO VFS-6841 switch to the clock module (all occurrences in this module)
).
-define(end_measurement(Name),
    put(Name, timer:now_diff(os:timestamp(), get(Name)))
).
-define(derive_measurement(From, Name, TransformFun),
    put(Name, TransformFun(get(From)))
).
-define(format_measurement(Name, Unit, Desc), #parameter{
    name = Name, unit = atom_to_list(Unit), description = Desc, value = case Unit of
        s -> get(Name) / 1000000;
        ms -> get(Name) / 1000;
        us -> get(Name)
    end
}).

-endif.
