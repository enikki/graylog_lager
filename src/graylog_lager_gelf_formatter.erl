-module(graylog_lager_gelf_formatter).
-author("silviu.caragea").

-export([format/2, format/3]).

-define(GELF_VERSION, <<"1.1">>).

format(Message, Config, _Colors) ->
    format(Message, Config).

format(Message, Config) ->
    RawData = get_raw_data(Message, Config),
    try
        JsonPayload = jiffy:encode({RawData}),
        do_compression(JsonPayload, graylog_lager_utils:lookup(compression, Config, disabled))
    catch
        _:_ ->
            RawData
    end.

get_raw_data(Message, Config) ->
    Meta = get_metadata(Message),
    Pid = proplists:get_value(<<"_pid">>, Meta),
    Meta1 = [{<<"_erlang_pid">>, Pid} | proplists:delete(<<"_pid">>, Meta)],
    BaseMessage = [
        {version, ?GELF_VERSION},
        {level, graylog_lager_utils:severity2int(lager_msg:severity(Message))},
        {short_message, graylog_lager_utils:term2bin(lager_msg:message(Message))},
        {timestamp, graylog_lager_utils:unix_timestamp(lager_msg:timestamp(Message))},
        {host, proplists:get_value(application_host, Config)} |
                   Meta1
    ],

    case graylog_lager_utils:lookup(extra_fields, Config) of
        undefined ->
            BaseMessage;
        Extra when is_list(Extra) ->
            BaseMessage ++ Extra
    end.

get_metadata(Msg) ->
    get_metadata(lager_msg:metadata(Msg), []).

get_metadata([], Acc) ->
    Acc;
get_metadata([{K,V}|T], Acc) ->
    NewK = <<"_", (graylog_lager_utils:term2bin(K))/binary>>,
    get_metadata(T, [{NewK, graylog_lager_utils:term2json(V)} | Acc]).

do_compression(Data, disabled) ->
    Data;
do_compression(Data, gzip) ->
    zlib:gzip(Data);
do_compression(Data, zlib) ->
    zlib:compress(Data).
