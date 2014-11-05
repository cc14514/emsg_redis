-module(emsg_redis_pool).

-behaviour(gen_server).

%% API
-export([start_link/1,get_conn/1,return_conn/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {pool,host,port,size}).

%% 清理连接的时间间隔
-define(CleanTime,1000*60*2).

%%%===================================================================
%%% API
%%%===================================================================
start_link({Name,Args}) ->
    gen_server:start_link({local, Name}, ?MODULE, [Args], []).

%% 获取一个连接
get_conn(PoolName)->
	gen_server:call(PoolName,get_conn).

%% 归还一个连接
return_conn(PoolName,Conn) ->
	gen_server:cast(PoolName,{return_conn,Conn}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
%% [{host,"192.168.2.12"},{port,6379},{size,10}]
init([Args]) ->
	io:format("emsg_redis_pool__init ::> ~p~n",[Args]),
	Conf = dict:from_list(Args),
	{ok,Host} = dict:find(host,Conf),
	{ok,Port} = dict:find(port,Conf),
	{ok,Size} = dict:find(size,Conf),
	Pool = build_pool(Size,Host,Port,queue:new()),									
	io:format("emsg_redis_pool__init ::> ~p~n",[Pool]),
    { ok , #state{host=Host,port=Port,size=Size,pool=Pool} , ?CleanTime }.

handle_call(_Request, _From, #state{host=H,port=P,pool=Pool}=State) ->
	case queue:out(Pool) of 
		{empty,_} ->
			%% Reply = {ok,Conn} | {error,Reason}
			Reply = new_conn(H,P),
    		{reply, Reply, State,?CleanTime};
		{{value,Conn},Pool2} ->
    		{reply, {ok,Conn}, State#state{pool=Pool2},?CleanTime} 
	end.

handle_cast({return_conn,Conn}, #state{pool=Pool}=State) ->
    {noreply,State#state{pool=queue:in(Conn,Pool)},?CleanTime};
handle_cast(_Msg, State) -> {noreply, State, ?CleanTime}.


handle_info(timeout,#state{pool=Pool,size=Size}=State) ->
    {noreply, State#state{pool=free(Pool,Size)}, ?CleanTime};
handle_info(_Info, State) ->
    {noreply, State, ?CleanTime}.



terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

build_pool(N,H,P,Q) when N > 0 ->
	{ok,Conn} = new_conn(H,P),
	Q2 = queue:in(Conn,Q),
	io:format("~p~n",[Q2]),
	build_pool(N-1,H,P,Q2);
build_pool(0,_,_,Q) ->
	Q.

%% TODO 创建连接 
new_conn(H,P) ->
	{ok,{H,P}}.

%% TODO 释放连接，如果连接数超过 Size
free(Pool,_Size) ->
	Pool.
