-module(erlcloud_bucket_policy).

%% @doc Helper functions for dealing with S3 bucket policies

-export([json_to_term/1,
         term_to_json/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(POLICY_VERSION, <<"2008-10-17">>).

-spec json_to_term(string()) -> term().
json_to_term(_JSON) ->
    ok.

-spec term_to_json(term()) -> string().
term_to_json(Policy) ->
    Id = {<<"Id">>, proplists:get_value(id, Policy, undefined)},
    Version = {<<"Version">>, proplists:get_value(version, Policy, ?POLICY_VERSION)},
    Issuer = {<<"Issuer">>, proplists:get_value(issuer, Policy, undefined)},
    Statements = {<<"Statement">>, statements_to_json(
                                     proplists:get_value(statements, Policy, []))},
    JsonTerms = [{Key, Value} || {Key, Value} <- [Version, Id, Issuer, Statements],
                                 Value /= undefined],
    binary_to_list(iolist_to_binary(erlcloud_mochijson2:encode({struct, JsonTerms}))).

-spec statements_to_json([term()]) -> undefined | [term()].
statements_to_json([]) ->
    undefined;
statements_to_json(Statements) ->
    [statement_to_json(Statement) || Statement <- Statements].

-spec statement_to_json(proplist:proplist()) -> {struct, [term()]}.
statement_to_json(Statement) ->
    Sid = {<<"Sid">>, proplists:get_value(sid, Statement, undefined)},
    Action = {<<"Action">>, proplists:get_value(action, Statement, [])},
    NotAction = {<<"NotAction">>, proplists:get_value(notaction, Statement, undefined)},
    Effect = {<<"Effect">>, proplists:get_value(effect, Statement, undefined)},

    Resource = {<<"Resource">>, proplists:get_value(resource, Statement, undefined)},
    Condition = {<<"Condition">>, conditions_to_json(
                                    proplists:get_value(conditions,
                                                        Statement,
                                                        undefined))},
    Principal = {<<"Principal">>, principal_to_json(
                                    proplists:get_value(principal, Statement, undefined))},
    JsonTerms = [{Key, Value} || {Key, Value} <- [Sid,
                                                  Action,
                                                  NotAction,
                                                  Effect,
                                                  Resource,
                                                  Condition,
                                                  Principal],
                                 Value /= undefined],
    {struct, JsonTerms}.

%% @doc Convert a set of policy conditions to json terms.
-type condition() :: {binary(), binary(), binary() | [binary()]}.
-spec conditions_to_json([condition()]) -> term().
conditions_to_json(Conditions) ->
    JsonTerms = [{Condition, {struct, [{Key, Value}]}} || {Condition, Key, Value} <- Conditions],
    {struct, JsonTerms}.

principal_to_json(Principal) when is_list(Principal) ->
    {struct, [{<<"AWS">>, Principal}]};
principal_to_json(Principal) ->
    {struct, [{<<"AWS">>, [Principal]}]}.

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

term_to_json_test() ->
    ExpectedPolicy = "{\"Version\":\"2008-10-17\",\"Id\":\"Policy123\",\"Statement\":[{\"Sid\":\"Stmt345\",\"Action\":[\"s3:CreateBucket\",\"s3:DeleteBucket\"],\"Effect\":\"Allow\",\"Resource\":\"arn:aws:s3:::test_bucket/*\",\"Condition\":{\"IpAddress\":{\"aws:SourceIp\":[\"127.0.0.1\",\"192.168.1.1\"]}},\"Principal\":{\"AWS\":[\"*.*\"]}}]}",
    Statements = [[{sid, <<"Stmt345">>},
                   {action, [<<"s3:CreateBucket">>, <<"s3:DeleteBucket">>]},
                   {effect, <<"Allow">>},
                   {resource, <<"arn:aws:s3:::test_bucket/*">>},
                   {conditions, [{<<"IpAddress">>,
                                <<"aws:SourceIp">>,
                                [<<"127.0.0.1">>, <<"192.168.1.1">>]}]},
                   {principal, <<"*.*">>}]],
    Policy = [{id, <<"Policy123">>},
              {version, ?POLICY_VERSION},
              {statements, Statements}],
    ?assertEqual(ExpectedPolicy, term_to_json(Policy)).

-endif.
