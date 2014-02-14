%% Amazon Simple Storage Service (S3)

-module(erlcloud_s3).
-export([new/2, new/3, new/4, new/5, new/8,
         configure/2, configure/3, configure/4, configure/5, configure/8,
         create_bucket/1, create_bucket/2, create_bucket/3,
         delete_bucket/1, delete_bucket/2,
         get_bucket_attribute/2, get_bucket_attribute/3,
         list_buckets/0, list_buckets/1,
         set_bucket_attribute/3, set_bucket_attribute/4,
         list_objects/1, list_objects/2, list_objects/3,
         list_object_versions/1, list_object_versions/2, list_object_versions/3,
         copy_object/4, copy_object/5, copy_object/6,
         delete_object/2, delete_object/3,
         delete_object_version/3, delete_object_version/4,
         get_object/2, get_object/3, get_object/4,
         head_object/2, head_object/3, head_object/4,
         get_object_acl/2, get_object_acl/3, get_object_acl/4,
         get_object_torrent/2, get_object_torrent/3,
         get_object_metadata/2, get_object_metadata/3, get_object_metadata/4,
         put_object/3, put_object/4, put_object/5, put_object/6,
	 put_object_img_jpeg/5,put_object_img_jpeg/6,
         put_multipart_object/6, put_multipart_file/5,
         set_object_acl/3, set_object_acl/4,
         make_link/3, make_link/4,
         s3_simple_request/8, s3_xml_request/8, s3_request/8]).

-include_lib("erlcloud/include/erlcloud.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-spec new(string(), string()) -> aws_config().
new(AccessKeyID, SecretAccessKey) ->
    #aws_config{
     access_key_id=AccessKeyID,
     secret_access_key=SecretAccessKey
    }.

-spec new(string(), string(), string()) -> aws_config().
new(AccessKeyID, SecretAccessKey, Host) ->
    #aws_config{
     access_key_id=AccessKeyID,
     secret_access_key=SecretAccessKey,
     s3_host=Host
    }.


-spec new(string(), string(), string(), non_neg_integer()) -> aws_config().
new(AccessKeyID, SecretAccessKey, Host, Port) ->
    #aws_config{
     access_key_id=AccessKeyID,
     secret_access_key=SecretAccessKey,
     s3_host=Host,
     s3_port=Port
    }.

-spec new(string(), string(), string(), non_neg_integer(), string()) -> aws_config().
new(AccessKeyID, SecretAccessKey, Host, Port, Protocol) ->
    #aws_config{
     access_key_id=AccessKeyID,
     secret_access_key=SecretAccessKey,
     s3_host=Host,
     s3_port=Port,
     s3_prot=Protocol
    }.

-spec new(string(),
          string(),
          string(),
          non_neg_integer(),
          string(),
          string(),
          non_neg_integer(),
          proplist()) -> aws_config().
new(AccessKeyID, SecretAccessKey, Host, Port, Protocol, ProxyHost, ProxyPort,
    HttpOptions) ->
    #aws_config{
     access_key_id=AccessKeyID,
     secret_access_key=SecretAccessKey,
     s3_host=Host,
     s3_port=Port,
     s3_prot=Protocol,
     http_options=[{proxy_host, ProxyHost}, {proxy_port, ProxyPort},
                   {max_sessions, 50}, {max_pipeline_size, 1},
                   {connect_timeout, 5000}, {inactivity_timeout, 240000}]
                  ++ HttpOptions
    }.

-spec configure(string(), string()) -> ok.
configure(AccessKeyID, SecretAccessKey) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey)),
    ok.

-spec configure(string(), string(), string()) -> ok.
configure(AccessKeyID, SecretAccessKey, Host) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey, Host)),
    ok.

-spec configure(string(), string(), string(), non_neg_integer()) -> ok.
configure(AccessKeyID, SecretAccessKey, Host, Port) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey, Host, Port)),
    ok.

-spec configure(string(), string(), string(), non_neg_integer(), string()) -> ok.
configure(AccessKeyID, SecretAccessKey, Host, Port, Protocol) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey, Host, Port, Protocol)),
    ok.

-spec configure(string(),
                string(),
                string(),
                non_neg_integer(),
                string(),
                string(),
                non_neg_integer(),
                proplist()) -> ok.
configure(AccessKeyID,
          SecretAccessKey,
          Host,
          Port,
          Protocol,
          ProxyHost,
          ProxyPort,
          HTTPOptions) ->
    put(aws_config, new(AccessKeyID,
                        SecretAccessKey,
                        Host,
                        Port,
                        Protocol,
                        ProxyHost,
                        ProxyPort,
                        HTTPOptions)),
    ok.

-type s3_bucket_attribute_name() :: acl
                                  | location
                                  | logging
                                  | request_payment
                                  | versioning.

-type s3_bucket_acl() :: private
                       | public_read
                       | public_read_write
                       | authenticated_read
                       | bucket_owner_read
                       | bucket_owner_full_control.

-type s3_location_constraint() :: none
                                | us_west_1
                                | eu.

-define(XMLNS_S3, "http://s3.amazonaws.com/doc/2006-03-01/").

-spec copy_object(string(), string(), string(), string()) -> proplist().

copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName) ->
    copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName, []).

-spec copy_object(string(), string(), string(), string(), proplist() | aws_config()) -> proplist().

copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName, Config)
  when is_record(Config, aws_config) ->
    copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName, [], Config);

copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName, Options) ->
    copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName,
                Options, default_config()).

-spec copy_object(string(), string(), string(), string(), proplist(), aws_config()) -> proplist().
copy_object(DestBucketName, DestKeyName, SrcBucketName, SrcKeyName, Options, Config) ->
    SrcVersion = case proplists:get_value(version_id, Options) of
                     undefined -> "";
                     VersionID -> ["?versionId=", VersionID]
                 end,
    RequestHeaders =
        [{"x-amz-copy-source", [SrcBucketName, $/, SrcKeyName, SrcVersion]},
         {"x-amz-metadata-directive", proplists:get_value(metadata_directive, Options)},
         {"x-amz-copy-source-if-match", proplists:get_value(if_match, Options)},
         {"x-amz-copy-source-if-none-match", proplists:get_value(if_none_match, Options)},
         {"x-amz-copy-source-if-unmodified-since", proplists:get_value(if_unmodified_since, Options)},
         {"x-amz-copy-source-if-modified-since", proplists:get_value(if_modified_since, Options)},
         {"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}],
    {Headers, _Body} = s3_request(Config, put, DestBucketName, [$/|DestKeyName],
                                  [], [], <<>>, RequestHeaders),
    [{copy_source_version_id, proplists:get_value("x-amz-copy-source-version-id", Headers, "false")},
     {version_id, proplists:get_value("x-amz-version-id", Headers, "null")}].

-spec create_bucket(string()) -> ok.

create_bucket(BucketName) ->
    create_bucket(BucketName, private).

-spec create_bucket(string(), s3_bucket_acl() | aws_config()) -> ok.

create_bucket(BucketName, Config)
  when is_record(Config, aws_config) ->
    create_bucket(BucketName, private, Config);

create_bucket(BucketName, ACL) ->
    create_bucket(BucketName, ACL, none).

-spec create_bucket(string(), s3_bucket_acl(), s3_location_constraint() | aws_config()) -> ok.

create_bucket(BucketName, ACL, Config)
  when is_record(Config, aws_config) ->
    create_bucket(BucketName, ACL, none, Config);

create_bucket(BucketName, ACL, LocationConstraint) ->
    create_bucket(BucketName, ACL, LocationConstraint, default_config()).

-spec create_bucket(string(), s3_bucket_acl(), s3_location_constraint(), aws_config()) -> ok.

create_bucket(BucketName, ACL, LocationConstraint, Config)
  when is_list(BucketName), is_atom(ACL), is_atom(LocationConstraint) ->
    Headers = case ACL of
                  private -> [];  %% private is the default
                  _       -> [{"x-amz-acl", encode_acl(ACL)}]
              end,
    POSTData = case LocationConstraint of
                   none -> {<<>>, "application/octet-stream"};
                   Location when Location =:= eu; Location =:= us_west_1 ->
                       LocationName = case Location of eu -> "EU"; us_west_1 -> "us-west-1" end,
                       XML = {'CreateBucketConfiguration', [{xmlns, ?XMLNS_S3}],
                              [{'LocationConstraint', [LocationName]}]},
                       list_to_binary(xmerl:export_simple([XML], xmerl_xml))
               end,
    s3_simple_request(Config, put, BucketName, "/", "", [], POSTData, Headers).

encode_acl(undefined)                 -> undefined;
encode_acl(private)                   -> "private";
encode_acl(public_read)               -> "public-read";
encode_acl(public_read_write)         -> "public-read-write";
encode_acl(authenticated_read)        -> "authenticated-read";
encode_acl(bucket_owner_read)         -> "bucket-owner-read";
encode_acl(bucket_owner_full_control) -> "bucket-owner-full-control".

-spec delete_bucket(string()) -> ok.

delete_bucket(BucketName) ->
    delete_bucket(BucketName, default_config()).

-spec delete_bucket(string(), aws_config()) -> ok.

delete_bucket(BucketName, Config)
  when is_list(BucketName) ->
    s3_simple_request(Config, delete, BucketName, "/", "", [], <<>>, []).

-spec delete_object(string(), string()) -> proplist().

delete_object(BucketName, Key) ->
    delete_object(BucketName, Key, default_config()).

-spec delete_object(string(), string(), aws_config()) -> proplist().

delete_object(BucketName, Key, Config)
  when is_list(BucketName), is_list(Key) ->
    {Headers, _Body} = s3_request(Config, delete, BucketName, [$/|Key], [], [], <<>>, []),
    Marker = proplists:get_value("x-amz-delete-marker", Headers, "false"),
    Id = proplists:get_value("x-amz-version-id", Headers, "null"),
    [{delete_marker, list_to_existing_atom(Marker)},
     {version_id, Id}].

-spec delete_object_version(string(), string(), string()) -> proplist().

delete_object_version(BucketName, Key, Version) ->
    delete_object_version(BucketName, Key, Version, default_config()).

-spec delete_object_version(string(), string(), string(), aws_config()) -> proplist().

delete_object_version(BucketName, Key, Version, Config)
  when is_list(BucketName),
       is_list(Key),
       is_list(Version)->
    {Headers, _Body} = s3_request(Config, delete, BucketName, [$/|Key],
                                  ["versionId=", Version], [], <<>>, []),
    Marker = proplists:get_value("x-amz-delete-marker", Headers, "false"),
    Id = proplists:get_value("x-amz-version-id", Headers, "null"),
    [{delete_marker, list_to_existing_atom(Marker)},
     {version_id, Id}].

-spec list_buckets() -> proplist().

list_buckets() ->
    list_buckets(default_config()).

-spec list_buckets(aws_config()) -> proplist().

list_buckets(Config) ->
    Doc = s3_xml_request(Config, get, "", "/", "", [], <<>>, []),
    Buckets = [extract_bucket(Node) || Node <- xmerl_xpath:string("/*/Buckets/Bucket", Doc)],
    [{buckets, Buckets}].

-spec list_objects(string()) -> proplist().

list_objects(BucketName) ->
    list_objects(BucketName, []).

-spec list_objects(string(), proplist() | aws_config()) -> proplist().

list_objects(BucketName, Config)
  when is_record(Config, aws_config) ->
    list_objects(BucketName, [], Config);

list_objects(BucketName, Options) ->
    list_objects(BucketName, Options, default_config()).

-spec list_objects(string(), proplist(), aws_config()) -> proplist().

list_objects(BucketName, Options, Config)
  when is_list(BucketName),
       is_list(Options) ->
    Params = [{"delimiter", proplists:get_value(delimiter, Options)},
              {"marker", proplists:get_value(marker, Options)},
              {"max-keys", proplists:get_value(max_keys, Options)},
              {"prefix", proplists:get_value(prefix, Options)}],
    Doc = s3_xml_request(Config, get, BucketName, "/", "", Params, <<>>, []),
    Attributes = [{name, "Name", text},
                  {prefix, "Prefix", text},
                  {marker, "Marker", text},
                  {delimiter, "Delimiter", text},
                  {max_keys, "MaxKeys", integer},
                  {is_truncated, "IsTruncated", boolean},
                  {contents, "Contents", fun extract_contents/1},
                  {common_prefixes, "CommonPrefixes", fun extract_common_prefixes/1}],
    erlcloud_xml:decode(Attributes, Doc).

extract_contents(Nodes) ->
    Attributes = [{key, "Key", text},
                  {last_modified, "LastModified", time},
                  {etag, "ETag", text},
                  {size, "Size", integer},
                  {storage_class, "StorageClass", text},
                  {owner, "Owner", fun extract_user/1}],
    [erlcloud_xml:decode(Attributes, Node) || Node <- Nodes].

extract_common_prefixes(Nodes) ->
    Attributes = [{prefix, "Prefix", text}],
    [erlcloud_xml:decode(Attributes, Node) || Node <- Nodes].

extract_user([Node]) ->
    Attributes = [{id, "ID", text},
                  {display_name, "DisplayName", optional_text}],
    erlcloud_xml:decode(Attributes, Node).

-spec get_bucket_attribute(string(), s3_bucket_attribute_name()) -> term().

get_bucket_attribute(BucketName, AttributeName) ->
    get_bucket_attribute(BucketName, AttributeName, default_config()).

-spec get_bucket_attribute(string(), s3_bucket_attribute_name(), aws_config()) -> term().

get_bucket_attribute(BucketName, AttributeName, Config)
  when is_list(BucketName), is_atom(AttributeName) ->
    Attr = case AttributeName of
               acl             -> "acl";
               location        -> "location";
               logging         -> "logging";
               request_payment -> "requestPayment";
               versioning      -> "versioning"
           end,
    Doc = s3_xml_request(Config, get, BucketName, "/", Attr, [], <<>>, []),
    case AttributeName of
        acl ->
            Attributes = [{owner, "Owner", fun extract_user/1},
                          {access_control_list, "AccessControlList/Grant", fun extract_acl/1}],
            erlcloud_xml:decode(Attributes, Doc);
        location ->
            erlcloud_xml:get_text("/LocationConstraint", Doc);
        logging ->
            case xmerl_xpath:string("/BucketLoggingStatus/LoggingEnabled", Doc) of
                [] ->
                    {enabled, false};
                [LoggingEnabled] ->
                    Attributes = [{target_bucket, "TargetBucket", text},
                                  {target_prefix, "TargetPrefix", text},
                                  {target_trants, "TargetGrants/Grant", fun extract_acl/1}],
                    [{enabled, true}|erlcloud_xml:decode(Attributes, LoggingEnabled)]
            end;
        request_payment ->
            case erlcloud_xml:get_text("/RequestPaymentConfiguration/Payer", Doc) of
                "Requester" -> requester;
                _           -> bucket_owner
            end;
        versioning ->
            case erlcloud_xml:get_text("/VersioningConfiguration/Status", Doc) of
                "Enabled"   -> enabled;
                "Suspended" -> suspended;
                _           -> disabled
            end
    end.

extract_acl(ACL) ->
    [extract_grant(Item) || Item <- ACL].

extract_grant(Node) ->
    [{grantee, extract_user(xmerl_xpath:string("Grantee", Node))},
     {permission, decode_permission(erlcloud_xml:get_text("Permission", Node))}].

encode_permission(full_control) -> "FULL_CONTROL";
encode_permission(write)        -> "WRITE";
encode_permission(write_acp)    -> "WRITE_ACP";
encode_permission(read)         -> "READ";
encode_permission(read_acp) -> "READ_ACP".

decode_permission("FULL_CONTROL") -> full_control;
decode_permission("WRITE")        -> write;
decode_permission("WRITE_ACP")    -> write_acp;
decode_permission("READ")         -> read;
decode_permission("READ_ACP")     -> read_acp.

-spec get_object(string(), string()) -> proplist().

get_object(BucketName, Key) ->
    get_object(BucketName, Key, []).

-spec get_object(string(), string(), proplist() | aws_config()) -> proplist().

get_object(BucketName, Key, Config)
  when is_record(Config, aws_config) ->
    get_object(BucketName, Key, [], Config);

get_object(BucketName, Key, Options) ->
    get_object(BucketName, Key, Options, default_config()).

-spec get_object(string(), string(), proplist(), aws_config()) -> proplist().

get_object(BucketName, Key, Options, Config) ->
    fetch_object(get, BucketName, Key, Options, Config).

-spec head_object(string(), string()) -> proplist().
head_object(BucketName, Key) ->
    head_object(BucketName, Key, []).

-spec head_object(string(), string(), proplist() | aws_config()) -> proplist().
head_object(BucketName, Key, Config)
  when is_record(Config, aws_config) ->
    head_object(BucketName, Key, [], Config);
head_object(BucketName, Key, Options) ->
    head_object(BucketName, Key, Options, default_config()).

-spec head_object(string(), string(), proplist(), aws_config()) -> proplist().
head_object(BucketName, Key, Options, Config) ->
    fetch_object(head, BucketName, Key, Options, Config).

-spec fetch_object(atom(), string(), string(), proplist(), aws_config()) -> proplist().
fetch_object(Method, BucketName, Key, Options, Config) ->
    RequestHeaders = [{"Range", proplists:get_value(range, Options)},
                      {"Accept", proplists:get_value(accept, Options)},
                      {"If-Modified-Since", proplists:get_value(if_modified_since, Options)},
                      {"If-Unmodified-Since", proplists:get_value(if_unmodified_since, Options)},
                      {"If-Match", proplists:get_value(if_match, Options)},
                      {"If-None-Match", proplists:get_value(if_none_match, Options)}],
    Subresource = case proplists:get_value(version_id, Options) of
                      undefined -> "";
                      Version   -> [{"versionId", Version}]
                  end,
    {Headers, Body} = s3_request(Config, Method, BucketName, [$/|Key], Subresource, [], <<>>, RequestHeaders, [{response_format, binary}]),
    [{etag, proplists:get_value("ETag", Headers)},
     {content_length, proplists:get_value("Content-Length", Headers)},
     {content_type, proplists:get_value("Content-Type", Headers)},
     {delete_marker, list_to_existing_atom(proplists:get_value("x-amz-delete-marker", Headers, "false"))},
     {version_id, proplists:get_value("x-amz-version-id", Headers, "null")},
     {content, Body},
     {headers, Headers} |
     extract_metadata(Headers)].

-spec get_object_acl(string(), string()) -> proplist().

get_object_acl(BucketName, Key) ->
    get_object_acl(BucketName, Key, default_config()).

-spec get_object_acl(string(), string(), proplist() | aws_config()) -> proplist().

get_object_acl(BucketName, Key, Config)
  when is_record(Config, aws_config) ->
    get_object_acl(BucketName, Key, [], Config);

get_object_acl(BucketName, Key, Options) ->
    get_object_acl(BucketName, Key, Options, default_config()).

-spec get_object_acl(string(), string(), proplist(), aws_config()) -> proplist().

get_object_acl(BucketName, Key, Options, Config)
  when is_list(BucketName), is_list(Key), is_list(Options) ->
    Subresource = case proplists:get_value(version_id, Options) of
                      undefined -> ["acl"];
                      Version   -> ["acl" , {"versionId", Version}]
                  end,
    Doc = s3_xml_request(Config, get, BucketName, [$/|Key], Subresource, [], <<>>, []),
    Attributes = [{owner, "Owner", fun extract_user/1},
                  {access_control_list, "AccessControlList/Grant", fun extract_acl/1}],
    erlcloud_xml:decode(Attributes, Doc).

-spec get_object_metadata(string(), string()) -> proplist().

get_object_metadata(BucketName, Key) ->
    get_object_metadata(BucketName, Key, []).

-spec get_object_metadata(string(), string(), proplist() | aws_config()) -> proplist().

get_object_metadata(BucketName, Key, Config)
  when is_record(Config, aws_config) ->
    get_object_metadata(BucketName, Key, [], Config);

get_object_metadata(BucketName, Key, Options) ->
    get_object_metadata(BucketName, Key, Options, default_config()).

-spec get_object_metadata(string(), string(), proplist(), proplist() | aws_config()) -> proplist().

get_object_metadata(BucketName, Key, Options, Config) ->
    RequestHeaders = [{"If-Modified-Since", proplists:get_value(if_modified_since, Options)},
                      {"If-Unmodified-Since", proplists:get_value(if_unmodified_since, Options)},
                      {"If-Match", proplists:get_value(if_match, Options)},
                      {"If-None-Match", proplists:get_value(if_none_match, Options)}],
    Subresource = case proplists:get_value(version_id, Options) of
                      undefined -> "";
                      Version   -> [{"versionId", Version}]
                  end,
    {Headers, _Body} = s3_request(Config, get, BucketName, [$/|Key], Subresource, [], <<>>, RequestHeaders),
    [{last_modified, proplists:get_value("last-modified", Headers)},
     {etag, proplists:get_value("etag", Headers)},
     {content_length, proplists:get_value("content-length", Headers)},
     {content_type, proplists:get_value("content-type", Headers)},
     {delete_marker, list_to_existing_atom(proplists:get_value("x-amz-delete-marker", Headers, "false"))},
     {version_id, proplists:get_value("x-amz-version-id", Headers, "false")},
     {headers, Headers} |
     extract_metadata(Headers)].

extract_metadata(Headers) ->
    [{Key, Value} || {["x-amz-meta-"|Key], Value} <- Headers].

-spec get_object_torrent(string(), string()) -> proplist().

get_object_torrent(BucketName, Key) ->
    get_object_torrent(BucketName, Key, default_config()).

-spec get_object_torrent(string(), string(), aws_config()) -> proplist().

get_object_torrent(BucketName, Key, Config) ->
    {Headers, Body} = s3_request(Config, get, BucketName, [$/|Key], ["torrent"], [], <<>>, [], [{body_format, binary}]),
    [{delete_marker, list_to_existing_atom(proplists:get_value("x-amz-delete-marker", Headers, "false"))},
     {version_id, proplists:get_value("x-amz-delete-marker", Headers, "false")},
     {torrent, Body}].

-spec list_object_versions(string()) -> proplist().

list_object_versions(BucketName) ->
    list_object_versions(BucketName, []).

-spec list_object_versions(string(), proplist() | aws_config()) -> proplist().

list_object_versions(BucketName, Config)
  when is_record(Config, aws_config) ->
    list_object_versions(BucketName, [], Config);

list_object_versions(BucketName, Options) ->
    list_object_versions(BucketName, Options, default_config()).

-spec list_object_versions(string(), proplist(), aws_config()) -> proplist().

list_object_versions(BucketName, Options, Config)
  when is_list(BucketName), is_list(Options) ->
    Params = [{"delimiter", proplists:get_value(delimiter, Options)},
              {"key-marker", proplists:get_value(key_marker, Options)},
              {"max-keys", proplists:get_value(max_keys, Options)},
              {"prefix", proplists:get_value(prefix, Options)},
              {"version-id-marker", proplists:get_value(version_id_marker, Options)}],
    Doc = s3_xml_request(Config, get, BucketName, "/", ["versions"], Params, <<>>, []),
    Attributes = [{name, "Name", text},
                  {prefix, "Prefix", text},
                  {key_marker, "KeyMarker", text},
                  {next_key_marker, "NextKeyMarker", optional_text},
                  {version_id_marker, "VersionIdMarker", text},
                  {next_version_id_marker, "NextVersionIdMarker", optional_text},
                  {max_keys, "MaxKeys", integer},
                  {is_truncated, "Istruncated", boolean},
                  {versions, "Version", fun extract_versions/1},
                  {delete_markers, "DeleteMarker", fun extract_delete_markers/1}],
    erlcloud_xml:decode(Attributes, Doc).

extract_versions(Nodes) ->
    [extract_version(Node) || Node <- Nodes].

extract_version(Node) ->
    Attributes = [{key, "Key", text},
                  {version_id, "VersionId", text},
                  {is_latest, "IsLatest", boolean},
                  {etag, "ETag", text},
                  {size, "Size", integer},
                  {owner, "Owner", fun extract_user/1},
                  {storage_class, "StorageClass", text}],
    erlcloud_xml:decode(Attributes, Node).

extract_delete_markers(Nodes) ->
    [extract_delete_marker(Node) || Node <- Nodes].

extract_delete_marker(Node) ->
    Attributes = [{key, "Key", text},
                  {version_id, "VersionId", text},
                  {is_latest, "IsLatest", boolean},
                  {owner, "Owner", fun extract_user/1}],
    erlcloud_xml:decode(Attributes, Node).

extract_bucket(Node) ->
    erlcloud_xml:decode([{name, "Name", text},
                         {creation_date, "CreationDate", time}],
                        Node).

-spec put_object(string(), string(), iolist()) -> proplist().

put_object(BucketName, Key, Value) ->
    put_object(BucketName, Key, Value, []).

-spec put_object(string(), string(), iolist(), proplist() | aws_config()) -> proplist().

put_object(BucketName, Key, Value, Config)
  when is_record(Config, aws_config) ->
    put_object(BucketName, Key, Value, [], Config);

put_object(BucketName, Key, Value, Options) ->
    put_object(BucketName, Key, Value, Options, default_config()).

-spec put_object(string(), string(), iolist(), proplist(), [{string(), string()}] | aws_config()) -> proplist().

put_object(BucketName, Key, Value, Options, Config)
  when is_record(Config, aws_config) ->
    put_object(BucketName, Key, Value, Options, [], Config);

put_object(BucketName, Key, Value, Options, HTTPHeaders) ->
    put_object(BucketName, Key, Value, Options, HTTPHeaders, default_config()).

-spec put_object(string(), string(), iolist(), proplist(), [{string(), string()}], aws_config()) -> proplist().

put_object(BucketName, Key, Value, Options, HTTPHeaders, Config)
  when is_list(BucketName), is_list(Key), is_list(Value) orelse is_binary(Value),
       is_list(Options) ->
    RequestHeaders = [{"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}|HTTPHeaders]
        ++ [{["x-amz-meta-"|string:to_lower(MKey)], MValue} ||
               {MKey, MValue} <- proplists:get_value(meta, Options, [])],
    ContentType = proplists:get_value("content-type", HTTPHeaders, "application/octet_stream"),
    ReturnResponse = proplists:get_value(return_response, Options, false),
    POSTData = {iolist_to_binary(Value), ContentType},
    {Headers, Body} = s3_request(Config, put, BucketName, [$/|Key], [], [],
                                 POSTData, RequestHeaders),
    case ReturnResponse of
        true ->
            {Headers, Body};
        false ->
            [{version_id, proplists:get_value("x-amz-version-id", Headers, "null")}]
    end.


put_object_img_jpeg(BucketName, Key, Value, Options, Config)
  when is_record(Config, aws_config) ->
    put_object_img_jpeg(BucketName, Key, Value, Options, [], Config).

put_object_img_jpeg(BucketName, Key, Value, Options, HTTPHeaders, Config) 
  when is_list(BucketName), is_list(Key), is_list(Value) orelse is_binary(Value),
       is_list(Options) ->
    RequestHeaders = [{"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}|HTTPHeaders]
        ++ [{["x-amz-meta-"|string:to_lower(MKey)], MValue} ||
               {MKey, MValue} <- proplists:get_value(meta, Options, [])],
    ContentType = proplists:get_value("content-type", HTTPHeaders, "image/jpeg"),%%"application/octet_stream"),
    ReturnResponse = proplists:get_value(return_response, Options, false),
    POSTData = {iolist_to_binary(Value), ContentType},
    {Headers, Body} = s3_request(Config, put, BucketName, [$/|Key], [], [],
                                 POSTData, RequestHeaders),
    case ReturnResponse of
        true ->
            {Headers, Body};
        false ->
            [{version_id, proplists:get_value("x-amz-version-id", Headers, "null")}]
    end.

-spec put_multipart_object(string(), string(), binary(), proplist(), [{string(), string()}], aws_config()) -> proplist().

put_multipart_object(BucketName, Key, Value, Options, HTTPHeaders, Config)
  when is_list(BucketName), is_list(Key), is_list(Value) orelse is_binary(Value),
       is_list(Options) ->
    RequestHeaders = [{"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}|HTTPHeaders]
        ++ [{["x-amz-meta-"|string:to_lower(MKey)], MValue} ||
               {MKey, MValue} <- proplists:get_value(meta, Options, [])],
    ContentType = proplists:get_value("content-type", HTTPHeaders, "application/octet_stream"),
    _ReturnResponse = proplists:get_value(return_response, Options, false),
    UploadId = erlcloud_s3_multipart:upload_id(
                 erlcloud_s3_multipart:initiate_upload(BucketName,
                                                       Key,
                                                       ContentType,
                                                       RequestHeaders,
                                                       Config)),
    {ok, EtagList} = erlcloud_s3_multipart:upload_parts(BucketName,
                                                        Key,
                                                        Value,
                                                        UploadId,
                                                        proplists:get_value(chunk_size, Options),
                                                       Config),
    erlcloud_s3_multipart:complete_upload(BucketName,
                                          Key,
                                          UploadId,
                                          EtagList,
                                          Config).

-spec put_multipart_file(string(), string(), proplist(), [{string(), string()}], aws_config()) -> proplist().

put_multipart_file(BucketName, FileName, Options, HTTPHeaders, Config)
  when is_list(BucketName), is_list(FileName), is_list(Options) ->
    RequestHeaders = [{"x-amz-acl", encode_acl(proplists:get_value(acl, Options))}|HTTPHeaders]
        ++ [{["x-amz-meta-"|string:to_lower(MKey)], MValue} ||
               {MKey, MValue} <- proplists:get_value(meta, Options, [])],
    ContentType = proplists:get_value("content-type", HTTPHeaders, "application/octet_stream"),
    _ReturnResponse = proplists:get_value(return_response, Options, false),
    UploadId = erlcloud_s3_multipart:upload_id(
                 erlcloud_s3_multipart:initiate_upload(BucketName,
                                                       FileName,
                                                       ContentType,
                                                       RequestHeaders,
                                                       Config)),
    {ok, EtagList} = erlcloud_s3_multipart:upload_file(BucketName,
                                                       FileName,
                                                       UploadId,
                                                       proplists:get_value(chunk_size, Options),
                                                       Config),
    erlcloud_s3_multipart:complete_upload(BucketName,
                                          FileName,
                                          UploadId,
                                          EtagList,
                                          Config).

-spec set_object_acl(string(), string(), proplist()) -> ok.

set_object_acl(BucketName, Key, ACL) ->
    set_object_acl(BucketName, Key, ACL, default_config()).

-spec set_object_acl(string(), string(), proplist(), aws_config()) -> ok.

set_object_acl(BucketName, Key, ACL, Config)
  when is_list(BucketName), is_list(Key), is_list(ACL) ->
    Id = proplists:get_value(id, proplists:get_value(owner, ACL)),
    DisplayName = proplists:get_value(display_name, proplists:get_value(owner, ACL)),
    ACL1 = proplists:get_value(access_control_list, ACL),
    XML = {'AccessControlPolicy',
           [{'Owner', [{'ID', [Id]}, {'DisplayName', [DisplayName]}]},
            {'AccessControlList', encode_grants(ACL1)}]},
    XMLText = list_to_binary(xmerl:export_simple([XML], xmerl_xml)),
    s3_simple_request(Config, put, BucketName, [$/|Key], ["acl"], [], XMLText, []).

-spec make_link(integer(), string(), string()) -> {integer(), string(), string()}.

make_link(Expire_time, BucketName, Key)
  when is_integer(Expire_time), is_list(BucketName), is_list(Key) ->
    make_link(Expire_time, BucketName, Key, default_config()).

-spec make_link(integer(), string(), string(), aws_config()) -> {integer(), string(), string()}.

make_link(Expire_time, BucketName, Key, Config)
  when is_integer(Expire_time), is_list(BucketName), is_list(Key) ->
    {Mega, Sec, _Micro} = os:timestamp(),
    Datetime = (Mega * 1000000) + Sec,
    Expires = integer_to_list(Expire_time + Datetime),
    To_sign = lists:flatten(["GET\n\n\n", Expires, "\n/", BucketName, "/", Key]),
    Sig = base64:encode(crypto:sha_mac(Config#aws_config.secret_access_key, To_sign)),
    Host = lists:flatten(["http://", BucketName, ".", Config#aws_config.s3_host, port_spec(Config)]),
    URI = lists:flatten(["/", Key, "?AWSAccessKeyId=", erlcloud_http:url_encode(Config#aws_config.access_key_id), "&Signature=", erlcloud_http:url_encode(Sig), "&Expires=", Expires]),
    {list_to_integer(Expires),
     binary_to_list(erlang:iolist_to_binary(Host)),
     binary_to_list(erlang:iolist_to_binary(URI))}.

-spec set_bucket_attribute(string(), atom(), term()) -> ok.

set_bucket_attribute(BucketName, AttributeName, Value) ->
    set_bucket_attribute(BucketName, AttributeName, Value, default_config()).

-spec set_bucket_attribute(string(), atom(), term(), aws_config()) -> ok.

set_bucket_attribute(BucketName, AttributeName, Value, Config)
  when is_list(BucketName) ->
    {Subresource, XML} =
        case AttributeName of
            acl ->
                ACLXML = {'AccessControlPolicy',
                          [{'Owner',
                            [{'ID', [proplists:get_value(id, proplists:get_value(owner, Value))]},
                             {'DisplayName', [proplists:get_value(display_name, proplists:get_value(owner, Value))]}]},
                           {'AccessControlList', encode_grants(proplists:get_value(access_control_list, Value))}]},
                {["acl"], ACLXML};
            logging ->
                LoggingXML = {'BucketLoggingStatus',
                              [{xmlns, ?XMLNS_S3}],
                              case proplists:get_bool(enabled, Value) of
                                  true ->
                                      [{'LoggingEnabled',
                                        [
                                         {'TargetBucket', [proplists:get_value(target_bucket, Value)]},
                                         {'TargetPrefix', [proplists:get_value(target_prefix, Value)]},
                                         {'TargetGrants', encode_grants(proplists:get_value(target_grants, Value, []))}
                                        ]
                                       }];
                                  false ->
                                      []
                              end},
                {["logging"], LoggingXML};
            request_payment ->
                PayerName = case Value of
                                requester -> "Requester";
                                bucket_owner -> "BucketOwner"
                            end,
                RPXML = {'RequestPaymentConfiguration', [{xmlns, ?XMLNS_S3}],
                         [
                          {'Payer', [PayerName]}
                         ]
                        },
                {["requestPayment"], RPXML};
            versioning ->
                Status = case proplists:get_value(status, Value) of
                             suspended -> "Suspended";
                             enabled -> "Enabled"
                         end,
                MFADelete = case proplists:get_value(mfa_delete, Value, disabled) of
                                enabled -> "Enabled";
                                disabled -> "Disabled"
                            end,
                VersioningXML = {'VersioningConfiguration', [{xmlns, ?XMLNS_S3}],
                                 [{'Status', [Status]},
                                  {'MfaDelete', [MFADelete]}]},
                {["versioning"], VersioningXML}
        end,
    POSTData = list_to_binary(xmerl:export_simple([XML], xmerl_xml)),
    Headers = [{"content-type", "application/xml"}],
    s3_simple_request(Config, put, BucketName, "/", Subresource, [], POSTData, Headers).

encode_grants(Grants) ->
    [encode_grant(Grant) || Grant <- Grants].

encode_grant(Grant) ->
    Grantee = proplists:get_value(grantee, Grant),
    {'Grant',
     [{'Grantee', [{xmlns, ?XMLNS_S3}],
       [{'ID', [proplists:get_value(id, proplists:get_value(owner, Grantee))]},
        {'DisplayName', [proplists:get_value(display_name, proplists:get_value(owner, Grantee))]}]},
      {'Permission', [encode_permission(proplists:get_value(permission, Grant))]}]}.

s3_simple_request(Config, Method, Host, Path, Subresource, Params, POSTData, Headers) ->
    case s3_request(Config, Method, Host, Path, Subresource, Params, POSTData, Headers) of
        {_Headers, ""} -> ok;
        {_Headers, Body} ->
            XML = element(1,xmerl_scan:string(Body)),
            case XML of
                #xmlElement{name='Error'} ->
                    ErrCode = erlcloud_xml:get_text("/Error/Code", XML),
                    ErrMsg = erlcloud_xml:get_text("/Error/Message", XML),
                    erlang:error({s3_error, ErrCode, ErrMsg});
                _ ->
                    ok
            end
    end.

s3_xml_request(Config, Method, Host, Path, Subresource, Params, POSTData, Headers) ->
    {_Headers, Body} = s3_request(Config, Method, Host, Path, Subresource, Params, POSTData, Headers),
    XML = element(1,xmerl_scan:string(Body)),
    case XML of
        #xmlElement{name='Error'} ->
            ErrCode = erlcloud_xml:get_text("/Error/Code", XML),
            ErrMsg = erlcloud_xml:get_text("/Error/Message", XML),
            erlang:error({s3_error, ErrCode, ErrMsg});
        _ ->
            XML
    end.

s3_request(Config, Method, Host, Path, Subresource, Params, POSTData, Headers) ->
    s3_request(Config, Method, Host, Path, Subresource, Params, POSTData, Headers, []).

s3_request(Config, Method, Host, Path, Subresources, Params, POSTData, Headers, GetOptions) ->
    {ContentMD5, ContentType, Body} =
        case POSTData of
            {PD, CT} -> {base64:encode(crypto:md5(PD)), CT, PD}; PD -> {"", "", PD}
        end,
    AmzHeaders = lists:filter(fun ({"x-amz-" ++ _, V}) when V =/= undefined -> true; (_) -> false end, Headers),
    Date = httpd_util:rfc1123_date(erlang:localtime()),
    EscapedPath = erlcloud_http:url_encode_loose(Path),
    Authorization = make_authorization(Config, Method, ContentMD5, ContentType,
                                       Date, AmzHeaders, Host, EscapedPath, Subresources),
    FHeaders = [Header || {_, Value} = Header <- Headers, Value =/= undefined],
    RequestHeaders = [{"date", Date}, {"authorization", Authorization}|FHeaders] ++
        case ContentMD5 of
            "" -> [];
            _ -> [{"content-md5", binary_to_list(ContentMD5)}]
        end,
    RequestURI = lists:flatten([
                                Config#aws_config.s3_prot,
                                "://",
                                case Host of "" -> ""; _ -> [Host, $.] end,
                                Config#aws_config.s3_host, port_spec(Config),
                                EscapedPath,
                                format_subresources(Subresources),
                                if
                                    Params =:= [] -> "";
                                    Subresources =:= [] -> [$?, erlcloud_http:make_query_string(Params)];
                                    true -> [$&, erlcloud_http:make_query_string(Params)]
                                end
                               ]),
    Timeout = 240000,
    Options = Config#aws_config.http_options,
    Response = case Method of
                   get ->
                       ibrowse:send_req(RequestURI, RequestHeaders, Method, [],
                                        Options ++ GetOptions, Timeout);
                   delete ->
                       ibrowse:send_req(RequestURI, RequestHeaders, Method,
                                       [], Options, Timeout);
                   _ ->
                       NewHeaders = [{"content-type", ContentType} | RequestHeaders],
                       ibrowse:send_req(RequestURI, NewHeaders, Method, Body,
                                     Options, Timeout)
               end,
    case Response of
        {ok, Status, ResponseHeaders, ResponseBody} ->
             S = list_to_integer(Status),
             case S >= 200 andalso S =< 299 of
                 true ->
                     {ResponseHeaders, ResponseBody};
                 false ->
                     erlang:error({aws_error, {http_error, S, "", ResponseBody}})
             end;
        {error, Error} ->
            erlang:error({aws_error, {socket_error, Error}})
    end.

make_authorization(Config, Method, ContentMD5, ContentType, Date, AmzHeaders,
                   Host, Resource, Subresources) ->
    CanonizedAmzHeaders =
        [[Name, $:, Value, $\n] || {Name, Value} <- lists:sort(AmzHeaders)],
    StringToSign = [string:to_upper(atom_to_list(Method)), $\n,
                    ContentMD5, $\n,
                    ContentType, $\n,
                    Date, $\n,
                    CanonizedAmzHeaders,
                    case Host of "" -> ""; _ -> [$/, Host] end,
                    Resource,
                    format_subresources(Subresources)

                   ],
    Signature = base64:encode(crypto:sha_mac(Config#aws_config.secret_access_key, StringToSign)),
    ["AWS ", Config#aws_config.access_key_id, $:, Signature].

format_subresources([]) ->
    [];
format_subresources(Subresources) ->
    [$? | string:join(lists:sort([format_subresource(Subresource) ||
                                     Subresource <- Subresources]),
                      "&")].

format_subresource({Subresource, Value}) when is_list(Value) ->
    Subresource ++ "=" ++ Value;
format_subresource({Subresource, Value}) when is_integer(Value) ->
    Subresource ++ "=" ++ integer_to_list(Value);
format_subresource(Subresource) ->
    Subresource.

default_config() -> erlcloud_aws:default_config().

port_spec(#aws_config{s3_port=80}) ->
    "";
port_spec(#aws_config{s3_port=Port}) ->
    [":", erlang:integer_to_list(Port)].
