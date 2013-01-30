-module(erlcloud_s3_multipart).

%% @doc Functions to support multipart uploading of documents using
%% the S3 API.

-import(erlcloud_aws , [default_config/0]).

-export([abort_upload/3,
         abort_upload/4,
         complete_upload/4,
         complete_upload/5,
         initiate_result_to_term/1,
         initiate_upload/4,
         initiate_upload/5,
         list_parts/5,
         list_uploads/3,
         parts_to_term/1,
         upload_file/5,
         upload_file/6,
         upload_id/1,
         upload_part/5,
         upload_part/6,
         upload_parts/6]).

-include_lib("erlcloud/include/erlcloud.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-define(DEFAULT_CHUNK_SIZE, 52428800). % 50 MB

%% ===================================================================
%% Public API
%% ===================================================================

-spec initiate_upload(string(), string(), string(), proplist()) -> proplist().
initiate_upload(BucketName, Key, ContentType, Headers) ->
    initiate_upload(BucketName, Key, ContentType, Headers, default_config()).

-spec initiate_upload(string(), string(), string(), proplist(), aws_config()) -> proplist().
initiate_upload(BucketName, Key, ContentType, Headers, Config)
  when is_list(BucketName), is_list(Key) ->
    Url = "/" ++ Key,
    erlcloud_s3:s3_xml_request(Config, post, BucketName, Url, ["uploads"], [], {[], ContentType}, Headers).

-type part_num() :: pos_integer().
-spec upload_part(string(), string(), string(), pos_integer(), binary()) -> {term(), term()}.
upload_part(BucketName, Key, UploadId, PartNum, PartData) ->
    upload_part(BucketName, Key, UploadId, PartNum, PartData, default_config()).

-spec upload_part(string(), string(), string(), pos_integer(), binary(), aws_config()) -> {term(), term()}.
upload_part(BucketName, Key, UploadId, PartNum, PartData, Config) ->
    Url = "/" ++ Key,
    Subresources = [{"partNumber", integer_to_list(PartNum)},
                    {"uploadId", UploadId}],
    Headers = [{"content-length", byte_size(PartData)}],
    erlcloud_s3:s3_request(Config, put, BucketName, Url, Subresources, [], {PartData, []}, Headers).

-type etag() :: string().
-type etag_list() :: [{part_num(), etag()}].
-spec complete_upload(string(), string(), string(), etag_list()) -> ok.
complete_upload(BucketName, Key, UploadId, EtagList) ->
    complete_upload(BucketName, Key, UploadId, EtagList, default_config()).

-spec complete_upload(string(), string(), string(), etag_list(), aws_config()) -> ok.
complete_upload(BucketName, Key, UploadId, EtagList, Config) ->
    Url = "/" ++ Key,
    Subresources = [{"uploadId", UploadId}],
    EtagXml = etag_list_to_xml(EtagList),
    erlcloud_s3:s3_simple_request(Config, post, BucketName, Url, Subresources, [], EtagXml, []).

-spec abort_upload(string(), string(), string()) -> ok.
abort_upload(BucketName, Key, UploadId) ->
    abort_upload(BucketName, Key, UploadId, default_config()).

-spec abort_upload(string(), string(), string(), aws_config()) -> ok.
abort_upload(BucketName, Key, UploadId, Config) ->
    Url = "/" ++ Key,
    Subresources = [{"uploadId", UploadId}],
    erlcloud_s3:s3_simple_request(Config, delete, BucketName, Url, Subresources, [], [], []).

%% @doc Split a value into a set of `ChunkSize' parts and upload them
%% sequentially. The chunk size is specified in bytes.
-spec upload_parts(string(), string(), binary(), string(), undefined | pos_integer(), aws_config()) -> {ok, proplist()}.
upload_parts(BucketName, Key, Value, UploadId, undefined, Config) ->
    upload_parts(BucketName, Key, Value, UploadId, ?DEFAULT_CHUNK_SIZE, Config);
upload_parts(BucketName, Key, Value, UploadId, ChunkSize, Config) ->
    upload_parts(BucketName, Key, Value, UploadId, ChunkSize, Config, 1, []).

%% @doc Upload a file using multipart upload. The chunk size should be
%% specified in bytes. Any path information in `FileName' is removed
%% to form the `Key' used to store the the file. To otherwise specify the
%% `Key', use `upload_file/6'.
-spec upload_file(string(), string(), string(), undefined | pos_integer(), aws_config()) -> {ok, proplist()} | {error, term()}.
upload_file(BucketName, FileName, UploadId, undefined, Config) ->
    upload_file(BucketName, FileName, UploadId, ?DEFAULT_CHUNK_SIZE, Config);
upload_file(BucketName, FileName, UploadId, ChunkSize, Config) ->
    case file:open(FileName, [raw, read_ahead, binary]) of
        {ok, FD} ->
            upload_file(BucketName, filename:basename(FileName), FD, next_file_part(FD, ChunkSize), UploadId, ChunkSize, Config, 1, []);
        {error, Reason}=Error ->
            error_logger:error_msg("Failed to open file ~s. Reason: ~p~n", [FileName, Reason]),
            Error
    end.

%% @doc Upload a file using multipart upload. The chunk size should be
%% specified in bytes.
-spec upload_file(string(), string(), string(), string(), undefined | pos_integer(), aws_config()) -> {ok, proplist()} | {error, term()}.
upload_file(BucketName, Key, FileName, UploadId, undefined, Config) ->
    upload_file(BucketName, Key, FileName, UploadId, ?DEFAULT_CHUNK_SIZE, Config);
upload_file(BucketName, Key, FileName, UploadId, ChunkSize, Config) ->
    case file:open(FileName, [raw, read_ahead, binary]) of
        {ok, FD} ->
            upload_file(BucketName, Key, FD, next_file_part(FD, ChunkSize), UploadId, ChunkSize, Config, 1, []);
        {error, Reason}=Error ->
            error_logger:error_msg("Failed to open file ~s. Reason: ~p~n", [FileName, Reason]),
            Error
    end.

%% @doc Parse the upload id for an initiated multipart upload from the
%% XML response
-spec upload_id(#xmlElement{}) -> string().
upload_id(Xml) ->
    get_element_value(
      filter_content_elements(Xml#xmlElement.content, 'UploadId')).

%% @doc List all the active multipart uploads for a bucket
-spec list_uploads(string(), proplist(), aws_config()) -> proplist().
list_uploads(BucketName, Options, Config) when is_list(BucketName),
                                               is_list(Options) ->
    %% erlcloud_s3:s3_xml_request(Config, get, BucketName, "/", ["uploads"], [], [], []).
    Params = [{"delimiter", proplists:get_value(delimiter, Options)},
              {"key-marker", proplists:get_value(key_marker, Options)},
              {"upload-id-marker", proplists:get_value(upload_id_marker, Options)},
              {"max-uploads", proplists:get_value(max_uploads, Options)},
              {"prefix", proplists:get_value(prefix, Options)}],
    Doc = erlcloud_s3:s3_xml_request(Config, get, BucketName, "/", ["uploads"], Params, <<>>, []),
    Attributes = [{bucket, "Bucket", text},
                  {prefix, "Prefix", text},
                  {key_marker, "KeyMarker", text},
                  {upload_id_marker, "UploadIdMarker", text},
                  {next_key_marker, "NextKeyMarker", text},
                  {next_upload_id_marker, "NextUploadIdMarker", text},
                  {delimiter, "Delimiter", text},
                  {max_uploads, "MaxUploads", integer},
                  {is_truncated, "IsTruncated", boolean},
                  {uploads, "Upload", fun extract_uploads/1},
                  {common_prefixes, "CommonPrefixes", fun extract_common_prefixes/1}],
    erlcloud_xml:decode(Attributes, Doc).

%% @doc List all the parts for a multipart upload
-spec list_parts(string(), string(), string(), proplist(), aws_config()) -> [string()].
list_parts(BucketName, Key, UploadId, _Options, Config) ->
    Url = "/" ++ Key,
    erlcloud_s3:s3_xml_request(Config, get, BucketName, Url, [{"uploadId", UploadId}], [], [], []).

%% @doc Convert the response XML from an `Initiate Multipart Upload'
%% request to an erlang term
-spec initiate_result_to_term(term()) -> term().
initiate_result_to_term(Xml) ->
    ElementNames = ['Bucket', 'Key', 'UploadId'],
    Elements = filter_content_elements(Xml#xmlElement.content, ElementNames),
    initiate_result_to_term(Elements, {[], [], []}).

%% @doc Convert the response XML from a `List Parts' request to an
%% erlang term
-spec parts_to_term(term()) -> term().
parts_to_term(Xml) ->
    ElementNames = ['Bucket', 'Key', 'UploadId', 'Part'],
    Elements = filter_content_elements(Xml#xmlElement.content, ElementNames),
    parts_to_term(Elements, {[], [], [], []}).

%% ===================================================================
%% Internal functions
%% ===================================================================

-spec etag_list_to_xml(etag_list()) -> binary().
etag_list_to_xml(EtagList) ->
    PartsXML = [part_to_xml(PartNum, Etag) || {PartNum, Etag} <- EtagList],
    XML = {'CompleteMultipartUpload', PartsXML},
    list_to_binary(xmerl:export_simple([XML], xmerl_xml)).

-spec part_to_xml(part_num(), etag()) -> term().
part_to_xml(PartNum, Etag) ->
    {'Part',
     [
      {'PartNumber', [integer_to_list(PartNum)]},
      {'ETag', [Etag]}
     ]
    }.

upload_parts(_, _, <<>>, _, _, _, _, EtagList) ->
    {ok, lists:reverse(EtagList)};
upload_parts(BucketName, Key, Value, UploadId, ChunkSize, Config, PartNum, EtagList) ->
    {PartData, RestValue} = next_part(Value, ChunkSize),
    {RespHeaders, _UploadRes} = upload_part(BucketName, Key, UploadId, PartNum, PartData, Config),
    PartEtag = proplists:get_value("etag", RespHeaders),
    UpdEtagList = [{PartNum, PartEtag} | EtagList],
    upload_parts(BucketName, Key, RestValue, UploadId, ChunkSize, Config, PartNum + 1, UpdEtagList).

-type file_read_result() :: {ok, string() | binary()} | eof | {error, term()}.
-spec upload_file(string(), string(), file:io_device(), file_read_result(), string(), undefined | pos_integer(), aws_config(), pos_integer(), proplist()) -> {ok, proplist()} | {error, term()}.
upload_file(_, _, _, eof, _, _, _, _, EtagList) ->
    {ok, lists:reverse(EtagList)};
upload_file(BucketName, Key, FD, {ok, PartData}, UploadId, ChunkSize, Config, PartNum, EtagList) ->
    {RespHeaders, _UploadRes} = upload_part(BucketName, Key, UploadId, PartNum, PartData, Config),
    PartEtag = proplists:get_value("etag", RespHeaders),
    UpdEtagList = [{PartNum, PartEtag} | EtagList],
    upload_file(BucketName,
                Key,
                FD,
                next_file_part(FD, ChunkSize),
                UploadId,
                ChunkSize,
                Config,
                PartNum + 1,
                UpdEtagList);
upload_file(_, _, _, {error, Reason}=Error, _, _, _, PartNum, _) ->
    error_logger:error_msg("Failed to upload part ~p. Reason: ~p~n", [PartNum, Reason]),
    Error.

next_part(Value, ChunkSize) when byte_size(Value) =< ChunkSize ->
    {Value, <<>>};
next_part(Value, ChunkSize) ->
    << PartData:ChunkSize/binary, RestValue/binary >> = Value,
    {PartData, RestValue}.

-spec next_file_part(file:io_device(), pos_integer()) -> file_read_result().
next_file_part(FD, ChunkSize) ->
    file:read(FD, ChunkSize).

%% @doc Filter XML content except for an element with a specified name
%% or list of names
filter_content_elements(ContentElements, Name) when is_atom(Name) ->
    [Element || Element <- ContentElements,
                Element#xmlElement.name =:= Name];
filter_content_elements(ContentElements, Names) when is_list(Names) ->
    [Element || Element <- ContentElements,
                 lists:member(Element#xmlElement.name, Names)].

%% @doc Get the text value of a parsed XML element
get_element_value([]) ->
    [];
get_element_value([Element]) ->
    get_element_value(Element);
get_element_value(Element) when is_record(Element, xmlElement) ->
    %% `xmerl' gets confused parsing the etag with embedded $"
    %% so we have to handle it like this
    case Element#xmlElement.content of
        [Content] ->
            Content#xmlText.value;
        Content ->
            lists:flatten([ContentPart#xmlText.value || ContentPart <- Content])
    end.

-spec initiate_result_to_term([#xmlElement{}], {string(), string(), string()}) -> proplist().
initiate_result_to_term([], {Bucket, Key, UploadId}) ->
    [{bucket, Bucket}, {key, Key}, {upload_id, UploadId}];
initiate_result_to_term([Element | RestElements], {_, Key, UploadId}) when Element#xmlElement.name =:= 'Bucket' ->
    Bucket = get_element_value(Element),
    initiate_result_to_term(RestElements, {Bucket, Key, UploadId});
initiate_result_to_term([Element | RestElements], {Bucket, _, UploadId}) when Element#xmlElement.name =:= 'Key' ->
    Key = get_element_value(Element),
    initiate_result_to_term(RestElements, {Bucket, Key, UploadId});
initiate_result_to_term([Element | RestElements], {Bucket, Key, _}) when Element#xmlElement.name =:= 'UploadId' ->
    UploadId = get_element_value(Element),
    initiate_result_to_term(RestElements, {Bucket, Key, UploadId}).

-spec parts_to_term([#xmlElement{}], {string(), string(), string(), [term()]}) -> proplist().
parts_to_term([], {Bucket, Key, UploadId, Parts}) ->
    [{bucket, Bucket}, {key, Key}, {upload_id, UploadId}, {parts, lists:sort(Parts)}];
parts_to_term([Element | RestElements], {_, Key, UploadId, Parts}) when Element#xmlElement.name =:= 'Bucket' ->
    Bucket = get_element_value(Element),
    parts_to_term(RestElements, {Bucket, Key, UploadId, Parts});
parts_to_term([Element | RestElements], {Bucket, _, UploadId, Parts}) when Element#xmlElement.name =:= 'Key' ->
    Key = get_element_value(Element),
    parts_to_term(RestElements, {Bucket, Key, UploadId, Parts});
parts_to_term([Element | RestElements], {Bucket, Key, _, Parts}) when Element#xmlElement.name =:= 'UploadId' ->
    UploadId = get_element_value(Element),
    parts_to_term(RestElements, {Bucket, Key, UploadId, Parts});
parts_to_term([Element | RestElements], {Bucket, Key, UploadId, Parts}) ->
    PartInfo = part_element_to_term(Element),
    parts_to_term(RestElements, {Bucket, Key, UploadId, [PartInfo | Parts]}).

-spec part_element_to_term(#xmlElement{}) -> {pos_integer(), proplist()}.
part_element_to_term(Element) ->
    ElementNames = ['PartNumber', 'ETag', 'Size'],
    Elements = filter_content_elements(Element#xmlElement.content, ElementNames),
    part_element_to_term(Elements, {1, [], []}).

-spec part_element_to_term([#xmlElement{}], {non_neg_integer(), string(), string()}) -> {pos_integer(), proplist()}.
part_element_to_term([], {PartNum, Etag, Size}) ->
    {PartNum, [{etag, Etag}, {size, Size}]};
part_element_to_term([Element | RestElements], {_, Etag, Size}) when Element#xmlElement.name =:= 'PartNumber' ->
    PartNum = list_to_integer(get_element_value(Element)),
    part_element_to_term(RestElements, {PartNum, Etag, Size});
part_element_to_term([Element | RestElements], {PartNum, _, Size}) when Element#xmlElement.name =:= 'ETag' ->
    Etag = get_element_value(Element),
    part_element_to_term(RestElements, {PartNum, Etag, Size});
part_element_to_term([Element | RestElements], {PartNum, Etag, _}) when Element#xmlElement.name =:= 'Size' ->
    Size = get_element_value(Element),
    part_element_to_term(RestElements, {PartNum, Etag, Size}).

extract_uploads(Nodes) ->
    Attributes = [{key, "Key", text},
                  {upload_id, "UploadId", text},
                  {storage_class, "StorageClass", text},
                  {initiated, "Initiated", time},
                  {initiator, "Initiator", fun extract_user/1},
                  {owner, "Owner", fun extract_user/1}],
    [erlcloud_xml:decode(Attributes, Node) || Node <- Nodes].

extract_common_prefixes(Nodes) ->
    Attributes = [{prefix, "Prefix", text}],
    [erlcloud_xml:decode(Attributes, Node) || Node <- Nodes].

extract_user([Node]) ->
    Attributes = [{id, "ID", text},
                  {display_name, "DisplayName", optional_text}],
    erlcloud_xml:decode(Attributes, Node).
