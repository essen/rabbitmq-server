%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_web_stomp_wt_handler).
%% @todo -behaviour(cowboy_webtransport).

-export([init/2]).
-export([webtransport_handle/2]).
-export([webtransport_info/2]).
-export([terminate/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbitmq_stomp/include/rabbit_stomp.hrl").
-include_lib("rabbitmq_stomp/include/rabbit_stomp_frame.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {
    heartbeat_mode,
    heartbeat,
    heartbeat_sup,
    parse_state,
    proc_state,
    state,
    conserve_resources,
    socket,
    peername,
    auth_hd,
    stats_timer,
    connection,

    bidi_stream
}).

-define(APP, rabbitmq_web_stomp).

init(Req0, Opts) ->
    try
    init1(Req0, Opts)
    catch C:E:S ->
        logger:error("CRASH ~p ~p ~p", [C, E, S]),
        erlang:raise(C, E, S)
    end.

init1(Req0, Opts) ->
    logger:error("~p: ~0p ~0p", [?FUNCTION_NAME, Req0, Opts]),

    {PeerAddr, _PeerPort} = maps:get(peer, Req0),
    %% @todo
%    {_, KeepaliveSup} = lists:keyfind(keepalive_sup, 1, Opts),
%    SockInfo = maps:get(proxy_header, Req0, undefined),
    Req = case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req0) of
        undefined  -> Req0;
        Protocols ->
            case filter_stomp_protocols(Protocols) of
                [] -> Req0;
                [StompProtocol|_] ->
                    cowboy_req:set_resp_header(<<"sec-websocket-protocol">>,
                        StompProtocol, Req0)
            end
    end,
%    WsOpts0 = proplists:get_value(ws_opts, Opts, #{}),
%    WsOpts  = maps:merge(#{compress => true}, WsOpts0),
    State0 = #state{
%        heartbeat_sup      = KeepaliveSup,
        heartbeat          = {none, none},
        heartbeat_mode     = heartbeat,
        state              = running,
        conserve_resources = false,
%        socket             = SockInfo,
        peername           = PeerAddr,
        auth_hd            = cowboy_req:header(<<"authorization">>, Req)
    },

    %% websocket_init
    process_flag(trap_exit, true),
    {ok, ProcessorState} = init_processor_state(State0),
    State = rabbit_event:init_stats_timer(
           State0#state{proc_state     = ProcessorState,
                        parse_state    = rabbit_stomp_frame:initial_state()},
           #state.stats_timer),
    %% --

    {cowboy_webtransport, Req, State}.

%-spec close_connection(pid(), string()) -> 'ok'.
%close_connection(Pid, Reason) ->
%    rabbit_log_connection:info("Web STOMP: will terminate connection process ~tp, reason: ~ts",
%                               [Pid, Reason]),
%    sys:terminate(Pid, Reason),
%    ok.

init_processor_state(#state{%socket=Sock, 
        peername=PeerAddr, auth_hd=AuthHd}) ->
    Self = self(),
    SendFun = fun(Data) ->
                      Self ! {send, Data},
                      ok
              end,

    SSLLogin = application:get_env(rabbitmq_stomp, ssl_cert_login, false),
    StompConfig0 = #stomp_configuration{ssl_cert_login = SSLLogin, implicit_connect = false},
    UseHTTPAuth = application:get_env(rabbitmq_web_stomp, use_http_auth, false),
    UserConfig = application:get_env(rabbitmq_stomp, default_user, undefined),
    StompConfig1 = rabbit_stomp:parse_default_user(UserConfig, StompConfig0),
    StompConfig2 = case UseHTTPAuth of
        true ->
            case AuthHd of
                undefined ->
                    %% We fall back to the default STOMP credentials.
                    StompConfig1#stomp_configuration{force_default_creds = true};
                _ ->
                    {basic, HTTPLogin, HTTPPassCode}
                        = cow_http_hd:parse_authorization(AuthHd),
                    StompConfig0#stomp_configuration{
                      default_login = HTTPLogin,
                      default_passcode = HTTPPassCode,
                      force_default_creds = true}
            end;
        false ->
            StompConfig1
    end,

    %% @todo
    AdapterInfo = 
    #amqp_adapter_info{protocol        = {'Web STOMP', 1}, %% @todo Might need a separate name.
                       name            = <<>>,
                       host            = {0,0,0,0},
                       port            = 123,
                       peer_host       = {0,0,0,0},
                       peer_port       = 456,
                       additional_info = [{ssl, false}]
    },

%    RealSocket = rabbit_net:unwrap_socket(Sock),
%    LoginNameFromCertificate = rabbit_stomp_reader:ssl_login_name(RealSocket, StompConfig2),
    LoginNameFromCertificate = <<"todo">>, %% @todo
    ProcessorState = rabbit_stomp_processor:initial_state(
        StompConfig2,
        {SendFun, AdapterInfo, LoginNameFromCertificate, PeerAddr}),
    {ok, ProcessorState}.

webtransport_handle(Event={stream_open, StreamID, bidi}, State) ->
    logger:error("~p: ~0p ~0p", [?FUNCTION_NAME, Event, State]),
    {[], State#state{bidi_stream=StreamID}};
webtransport_handle(Event={stream_data, StreamID, _IsFin, Data},
        State=#state{bidi_stream=StreamID}) ->
    logger:error("~p: ~0p ~0p", [?FUNCTION_NAME, Event, State]),
    handle_data(Data, State);
webtransport_handle(Event, State) ->
    logger:error("~p: ~0p ~0p", [?FUNCTION_NAME, Event, State]),
    {[], State}.

webtransport_info(Event, State) ->
    logger:error("~p: ~0p ~0p", [?FUNCTION_NAME, Event, State]),
    webtransport_info1(Event, State).

webtransport_info1({send, Msg}, State=#state{bidi_stream=StreamID}) ->
    {[{send, StreamID, nofin, Msg}], State};

webtransport_info1({conserve_resources, Conserve}, State) ->
    NewState = State#state{conserve_resources = Conserve},
    handle_credits(control_throttle(NewState));
webtransport_info1({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    handle_credits(control_throttle(State));

webtransport_info1(#'basic.consume_ok'{}, State) ->
    {[], State};
webtransport_info1(#'basic.cancel_ok'{}, State) ->
    {[], State};
webtransport_info1(#'basic.ack'{delivery_tag = Tag, multiple = IsMulti},
               State=#state{ proc_state = ProcState0 }) ->
    ProcState = rabbit_stomp_processor:flush_pending_receipts(Tag,
                                                              IsMulti,
                                                              ProcState0),
    {[], State#state{ proc_state = ProcState }};
webtransport_info1({Delivery = #'basic.deliver'{},
               #amqp_msg{props = Props, payload = Payload},
               DeliveryCtx},
               State=#state{ proc_state = ProcState0 }) ->
    ProcState = rabbit_stomp_processor:send_delivery(Delivery,
                                                     Props,
                                                     Payload,
                                                     DeliveryCtx,
                                                     ProcState0),
    {[], State#state{ proc_state = ProcState }};
webtransport_info1({Delivery = #'basic.deliver'{},
               #amqp_msg{props = Props, payload = Payload}},
               State=#state{ proc_state = ProcState0 }) ->
    ProcState = rabbit_stomp_processor:send_delivery(Delivery,
                                                     Props,
                                                     Payload,
                                                     undefined,
                                                     ProcState0),
    {[], State#state{ proc_state = ProcState }};
webtransport_info1(#'basic.cancel'{consumer_tag = Ctag},
               State=#state{ proc_state = ProcState0 }) ->
    case rabbit_stomp_processor:cancel_consumer(Ctag, ProcState0) of
      {ok, ProcState, _Connection} ->
        {[], State#state{ proc_state = ProcState }};
      {stop, _Reason, ProcState} ->
        stop(State#state{ proc_state = ProcState })
    end;

webtransport_info1({start_heartbeats, _},
               State = #state{heartbeat_mode = no_heartbeat}) ->
    {[], State};

webtransport_info1({start_heartbeats, {0, 0}}, State) ->
    {[], State};
%% @todo
%webtransport_info1({start_heartbeats, {SendTimeout, ReceiveTimeout}},
%               State = #state{socket         = Sock,
%                              heartbeat_sup  = SupPid,
%                              heartbeat_mode = heartbeat}) ->
%    Self = self(),
%    SendFun = fun () -> Self ! {send, <<$\n>>}, ok end,
%    ReceiveFun = fun() -> Self ! client_timeout end,
%    Heartbeat = rabbit_heartbeat:start(SupPid, Sock, SendTimeout,
%                                       SendFun, ReceiveTimeout, ReceiveFun),
%    {[], State#state{heartbeat = Heartbeat}};
webtransport_info1({start_heartbeats, _}, State) ->
    {[], State};
webtransport_info1(client_timeout, State) ->
    stop(State);

%%----------------------------------------------------------------------------
webtransport_info1({'EXIT', From, Reason},
               State=#state{ proc_state = ProcState0 }) ->
  case rabbit_stomp_processor:handle_exit(From, Reason, ProcState0) of
    {stop, _Reason, ProcState} ->
        stop(State#state{ proc_state = ProcState });
    unknown_exit ->
        %% Allow the server to send remaining error messages
        self() ! close_websocket,
        {[], State}
  end;
webtransport_info1(close_websocket, State) ->
    stop(State);

%%----------------------------------------------------------------------------

webtransport_info1(emit_stats, State) ->
    {[], emit_stats(State)};

webtransport_info1(Msg, State) ->
    rabbit_log_connection:info("Web STOMP: unexpected message ~tp",
                    [Msg]),
    {[], State}.

terminate(Reason, Req, State=#state{proc_state = undefined}) ->
    logger:error("~p: ~0p ~0p ~0p", [?FUNCTION_NAME, Reason, Req, State]),
    ok;
terminate(Reason, Req, State=#state{proc_state = ProcState}) ->
    logger:error("~p: ~0p ~0p ~0p", [?FUNCTION_NAME, Reason, Req, State]),
    _ = rabbit_stomp_processor:flush_and_die(ProcState),
    ok.

%%----------------------------------------------------------------------------

%% The protocols v10.stomp, v11.stomp and v12.stomp are registered
%% at IANA: https://www.iana.org/assignments/websocket/websocket.xhtml

filter_stomp_protocols(Protocols) ->
    lists:reverse(lists:sort(lists:filter(
        fun(<< "v1", C, ".stomp">>)
            when C =:= $2; C =:= $1; C =:= $0 -> true;
           (_) ->
            false
        end,
        Protocols))).

%%----------------------------------------------------------------------------

handle_data(Data, State0) ->
    case handle_data1(Data, State0) of
        {[], State1 = #state{state = blocked}} ->
            %% @todo We currently don't have flow control in WebTransport.
            %{[{active, false}], State1};
            {[], State1};
        {error, Error0} ->
            Error1 = rabbit_misc:format("~tp", [Error0]),
            rabbit_log_connection:error("STOMP detected framing error '~ts'", [Error1]),
            stop(State0, 1007, Error1);
        %% @todo What can Other be? We need to convert to WT commands.
        Other ->
            Other
    end.

handle_data1(<<>>, State) ->
    {[], ensure_stats_timer(State)};
handle_data1(Bytes, State = #state{proc_state  = ProcState,
                                   parse_state = ParseState}) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            {[], ensure_stats_timer(State#state{ parse_state = ParseState1 })};
        {ok, Frame, Rest} ->
            case rabbit_stomp_processor:process_frame(Frame, ProcState) of
                {ok, ProcState1, ConnPid} ->
                    ParseState1 = rabbit_stomp_frame:initial_state(),
                    State1 = maybe_block(State, Frame),
                    logger:error("~p: ~0p ~0p", [?FUNCTION_NAME, Bytes, ProcState1]),
                    handle_data1(
                      Rest,
                      State1 #state{ parse_state = ParseState1,
                                     proc_state  = ProcState1,
                                     connection  = ConnPid });
                {stop, _Reason, ProcState1} ->
                    %% do not exit here immediately, because we need to wait for messages eventually enqueued by process_request
                    self() ! close_websocket,
                    {[], State#state{ proc_state = ProcState1 }}
            end;
        Other ->
            Other
    end.

maybe_block(State = #state{state = blocking, heartbeat = Heartbeat},
            #stomp_frame{command = "SEND"}) ->
    rabbit_heartbeat:pause_monitor(Heartbeat),
    State#state{state = blocked};
maybe_block(State, _) ->
    State.

stop(State) ->
    stop(State, 1000, "STOMP died").

stop(State = #state{proc_state = ProcState}, CloseCode, Error0) ->
    maybe_emit_stats(State),
    _ = rabbit_stomp_processor:flush_and_die(ProcState),
    Error1 = rabbit_data_coercion:to_binary(Error0),
    {[{close, CloseCode, Error1}], State}.

%%----------------------------------------------------------------------------

handle_credits(State0) ->
    case control_throttle(State0) of
        State = #state{state = running} ->
            %% @todo We currently don't have flow control in WebTransport.
            %{[{active, true}], State};
            {[], State};
        State ->
            {[], State}
    end.

control_throttle(State = #state{state              = CS,
                                conserve_resources = Mem}) ->
    case {CS, Mem orelse credit_flow:blocked()} of
        {running,   true} -> blocking(State);
        {blocking, false} -> running(State);
        {blocked,  false} -> running(State);
        {_,            _} -> State
    end.

blocking(State) ->
    State#state{state = blocking}.

running(State = #state{heartbeat=Heartbeat}) ->
    rabbit_heartbeat:resume_monitor(Heartbeat),
    State#state{state = running}.

%%----------------------------------------------------------------------------

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats).

maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #state.stats_timer,
                                fun() -> emit_stats(State) end).

emit_stats(State=#state{connection = C}) when C == none; C == undefined ->
    %% Avoid emitting stats on terminate when the connection has not yet been
    %% established, as this causes orphan entries on the stats database
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    State1;
emit_stats(State=#state{socket=Sock, state=RunningState, connection=Conn}) ->
    SockInfos = case rabbit_net:getstat(Sock,
            [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]) of
        {ok,    SI} -> SI;
        {error,  _} -> []
    end,
    Infos = [{pid, Conn}, {state, RunningState}|SockInfos],
    rabbit_core_metrics:connection_stats(Conn, Infos),
    rabbit_event:notify(connection_stats, Infos),
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    State1.
