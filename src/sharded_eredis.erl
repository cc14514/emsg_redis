%%%-------------------------------------------------------------------
%%% @author Jeremy Ong <jeremy@playmesh.com>
%%% @copyright (C) 2012, PlayMesh, Inc.
%%%-------------------------------------------------------------------

-module(sharded_eredis).

%% Include
-include_lib("eunit/include/eunit.hrl").

%% Default timeout for calls to the client gen_server
%% Specified in http://www.erlang.org/doc/man/gen_server.html#call-3
-define(TIMEOUT, 5000).

-export([start/0, stop/0]).

%% API
-export([q/1]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

%% @doc Query the redis server using eredis. Automatically hashes
%% the key and selects the correct shard
-spec q(Command::iolist()) ->
    {ok, binary() | [binary()]} | {error, Reason::binary()}.
q(Command) ->
    q(Command, ?TIMEOUT).

-spec q(Command::iolist(), Timeout::integer()) ->
    {ok, binary() | [binary()]} | {error, Reason::binary()}.
q(Command = [_, Key|_], Timeout) ->
    Node = sharded_eredis_chash:lookup(Key),
    poolboy:transaction(Node, fun(Worker) -> 
		eredis:q(Worker, Command, Timeout) 
	end).
