-module(boss_cache).
-export([start/0, start/1]).
-export([stop/0]).
-export([get/2, set/4, delete/2]).
-export([terminate/1]).

-define(BOSS_CACHE_TABLE, boss_cache).

-record(state, {
        adapter,
        connection
    }).

start() ->
    Adapter = boss_cache_adapter_memcached_bin,
    start([{adapter, Adapter}, {cache_servers, [{"127.0.0.1", 11211, 1}]}]).

start(Options) ->
    AdapterName = proplists:get_value(adapter, Options, memcached_bin),
    Adapter = list_to_atom(lists:concat(["boss_cache_adapter_", AdapterName])),
    Adapter:start(Options),
    {ok, Conn} = Adapter:init(Options),
    State = #state{ adapter = Adapter, connection = Conn },
    setup_table(),
    set_state(State),
    {ok, State}.

stop() ->
    teardown_table(),
    ok.

setup_table() ->
    case ets:info(?BOSS_CACHE_TABLE) of
        undefined -> ets:new(?BOSS_CACHE_TABLE, [set, public, named_table, {read_concurrency, true}]);
        _X -> ets:delete_all_objects(?BOSS_CACHE_TABLE)
    end.

teardown_table() ->
    case ets:info(?BOSS_CACHE_TABLE) of
        undefined -> ok;
        _X ->
            ets:delete(?BOSS_CACHE_TABLE)
    end.

get_state() ->
    [{state, State}] = ets:lookup(?BOSS_CACHE_TABLE, state),
    State.

set_state(State) ->
    ets:insert(?BOSS_CACHE_TABLE, {state, State}).

set(Prefix, Key, Val, TTL) ->
    State = get_state(),
    Adapter = State#state.adapter,
    Conn = State#state.connection,
    Adapter:set(Conn, Prefix, Key, Val, TTL).

get(Prefix, Key) ->
    State = get_state(),
    Adapter = State#state.adapter,
    Conn = State#state.connection,
    Adapter:get(Conn, Prefix, Key).

delete(Prefix, Key) ->
    State = get_state(),
    Adapter = State#state.adapter,
    Conn = State#state.connection,
    Adapter:delete(Conn, Prefix, Key).

terminate(_Reason) ->
    State = get_state(),
    Adapter = State#state.adapter,
    Conn = State#state.connection,
    Adapter:terminate(Conn).

