# Read-only reducer for the first qualifying Broadcastify Calls SID poll.
#
# Invocation owns the approved SID identities:
#   jq --argjson EXPECTED_SIDS '["..."]' -f bootstrap.jq raw.json
#
# Input is one object with complete collection metadata and raw Cloud Logging
# entries.  Collection and hashing happen before this reducer runs.

def fail($message):
  error("bcfy_calls_sid bootstrap evidence rejected: \($message)");

def require($condition; $message):
  if $condition then . else fail($message) end;

def is_integer:
  type == "number" and isfinite and floor == .;

def is_nonnegative_integer:
  is_integer and . >= 0;

def is_nonnegative_number:
  type == "number" and isfinite and . >= 0;

def is_nonempty_string:
  type == "string" and length > 0;

def timestamp_epoch($value; $name):
  if ($value | type) != "string" then
    fail("\($name) must be a UTC RFC3339 string")
  else
    try (
      $value
      | capture(
          "^(?<whole>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:" +
          "[0-9]{2}:[0-9]{2})(?<fraction>\\.[0-9]{1,9})?Z$"
        )
      | ((.whole + "Z" | fromdateiso8601) +
         ((.fraction // "0") | tonumber))
    ) catch fail("\($name) must be a valid UTC RFC3339 timestamp")
  end;

def exact_sorted_unique_strings($values; $name):
  $values
  | require(type == "array"; "\($name) must be an array")
  | require(length > 0; "\($name) must not be empty")
  | require(all(.[]; is_nonempty_string); "\($name) must contain strings")
  | require(. == (sort | unique); "\($name) must be sorted and unique");

def validate_raw_entry($entry; $frozen_instances; $start; $end):
  $entry
  | require(type == "object"; "every entry must be an object")
  | require(
      .insertId | is_nonempty_string;
      "every entry needs a nonempty insertId"
    )
  | (timestamp_epoch(.timestamp; "entry.timestamp")) as $entry_time
  | require(
      $entry_time >= $start and $entry_time < $end;
      "an entry is outside the captured half-open window"
    )
  | require(
      .resource.type == "gce_instance";
      "every entry must be scoped to a GCE instance"
    )
  | require(
      .resource.labels.instance_id | is_nonempty_string;
      "an entry is missing its numeric instance scope"
    )
  | require(
      ($frozen_instances | index($entry.resource.labels.instance_id)) != null;
      "an entry is outside the frozen instance-ID scope"
    )
  | require(
      .jsonPayload | type == "object";
      "every entry needs a structured jsonPayload"
    )
  | require(
      .jsonPayload.event_type == "bcfy_calls_sid_poll";
      "bootstrap input contains an unexpected event type"
    )
  | true;

def validate_collection($document; $event_types):
  $document
  | require(type == "object"; "input must be an object")
  | require(has("collection"); "collection metadata is missing")
  | require(has("entries"); "raw entries are missing")
  | require(.entries | type == "array"; "entries must be an array")
  | .collection as $collection
  | require($collection | type == "object"; "collection must be an object")
  | require(
      $collection.schema_version == 1;
      "collection schema_version must be 1"
    )
  | require(
      $collection.collection_complete == true;
      "collection completion marker is missing"
    )
  | require(
      $collection.limit_reached == false;
      "collection reports a reached result limit"
    )
  | require(
      $collection.result_limit | is_nonnegative_integer and . > 0;
      "result_limit must be a positive integer"
    )
  | require(
      $collection.returned_count | is_nonnegative_integer;
      "returned_count must be a nonnegative integer"
    )
  | require(
      $collection.returned_count == (.entries | length);
      "returned_count does not match the raw entry count"
    )
  | require(
      $collection.returned_count < $collection.result_limit;
      "returned_count reached the configured result limit"
    )
  | require(
      $collection.query_event_types == $event_types;
      "query_event_types does not match the reducer population"
    )
  | (exact_sorted_unique_strings(
       $collection.frozen_instance_ids;
       "frozen_instance_ids"
     )) as $frozen_instances
  | (exact_sorted_unique_strings(
       $collection.filter_instance_ids;
       "filter_instance_ids"
     )) as $filtered_instances
  | require(
      $frozen_instances == $filtered_instances;
      "the query does not contain the exact frozen instance-ID scope"
    )
  | require(
      $collection.page_count | is_nonnegative_integer and . > 0;
      "page_count must be a positive integer"
    )
  | require(
      $collection.pages | type == "array" and length > 0;
      "page metadata is missing"
    )
  | require(
      $collection.page_count == ($collection.pages | length);
      "page_count does not match page metadata"
    )
  | $collection.pages as $pages
  | require(
      all(
        range(0; $pages | length);
        . as $index |
        ($pages[$index] | type == "object") and
        ($pages[$index] | has("request_page_token")) and
        ($pages[$index] | has("next_page_token")) and
        ($pages[$index] | has("page_complete")) and
        ($pages[$index].page_number == ($index + 1)) and
        ($pages[$index].entry_count | is_nonnegative_integer)
      );
      "page metadata is malformed"
    )
  | require(
      ($pages | map(.entry_count) | add) == $collection.returned_count;
      "page entry counts do not match returned_count"
    )
  | ($pages | map(.request_page_token) | map(select(. != null)))
    as $request_page_tokens
  | ($pages | map(.next_page_token) | map(select(. != null)))
    as $next_page_tokens
  | require(
      ($request_page_tokens | length) ==
      ($request_page_tokens | unique | length) and
      ($next_page_tokens | length) ==
      ($next_page_tokens | unique | length);
      "page-token chain repeats or cycles"
    )
  | require(
      all(
        range(0; $pages | length);
        . as $index |
        if $index == 0 then
          $pages[$index].request_page_token == null
        else
          ($pages[$index - 1].next_page_token | is_nonempty_string) and
          ($pages[$index].request_page_token ==
           $pages[$index - 1].next_page_token)
        end
      );
      "page-token chain is incomplete"
    )
  | require(
      all(
        range(0; $pages | length);
        . as $index |
        if $index == (($pages | length) - 1) then
          $pages[$index].next_page_token == null and
          $pages[$index].page_complete == true
        else
          ($pages[$index].next_page_token | is_nonempty_string) and
          $pages[$index].page_complete == false
        end
      );
      "final-page completion marker is missing"
    )
  | (timestamp_epoch($collection.window_start; "collection.window_start"))
    as $window_start
  | (timestamp_epoch($collection.window_end; "collection.window_end"))
    as $window_end
  | require(
      $window_start < $window_end;
      "collection window must be a nonempty half-open interval"
    )
  | require(
      all(
        .entries[];
        validate_raw_entry(
          .;
          $frozen_instances;
          $window_start;
          $window_end
        )
      );
      "raw bootstrap entry validation failed"
    );

def validate_poll_entry:
  .jsonPayload as $payload
  | require(
      $payload.schema_version == 1;
      "poll schema_version must be 1"
    )
  | require($payload.sid | is_nonempty_string; "poll sid is malformed")
  | require(
      $payload.source_type == "bcfy_calls";
      "poll source_type is malformed"
    )
  | require(
      $payload.owner_worker_id | is_nonempty_string;
      "poll owner_worker_id is malformed"
    )
  | require(
      $payload.fencing_token | is_nonnegative_integer;
      "poll fencing_token is malformed"
    )
  | require(
      $payload.outcome |
      IN(
        "success",
        "membership_uncertain",
        "membership_invalid",
        "provider_failed",
        "page_failed",
        "stopped",
        "authority_lost"
      );
      "poll outcome is malformed"
    )
  | require(
      ($payload.provider_observed | type) == "boolean";
      "poll provider_observed is malformed"
    )
  | require(
      $payload.http_attempt_count | is_nonnegative_integer;
      "poll http_attempt_count is malformed"
    )
  | require(
      $payload | has("response_row_count") and
      has("response_distinct_audio_url_count") and
      has("response_last_pos") and has("request_pos");
      "poll response scalar evidence is incomplete"
    )
  | require(
      $payload.response_last_pos_state |
      IN("not_observed", "missing", "invalid", "regressive", "valid");
      "poll response_last_pos_state is malformed"
    )
  | require(
      $payload.request_pos == null or
      ($payload.request_pos | is_nonnegative_integer);
      "poll request_pos is malformed"
    )
  | if $payload.provider_observed then
      require(
        $payload.response_row_count | is_nonnegative_integer;
        "observed poll response_row_count is malformed"
      )
      | require(
          $payload.response_distinct_audio_url_count |
          is_nonnegative_integer;
          "observed poll distinct-URL denominator is absent or malformed"
        )
      | require(
          $payload.response_distinct_audio_url_count <=
          $payload.response_row_count;
          "observed poll distinct-URL count exceeds response rows"
        )
      | require(
          $payload.response_last_pos_state != "not_observed";
          "observed poll has no lastPos state"
        )
      | if $payload.response_last_pos_state == "valid" then
          require(
            $payload.response_last_pos | is_nonnegative_integer;
            "valid poll response_last_pos is malformed"
          )
          | require(
              $payload.request_pos == null or
              $payload.response_last_pos >= $payload.request_pos;
              "valid poll response_last_pos regresses request_pos"
            )
        else
          require(
            $payload.response_last_pos == null;
            "non-valid poll retains response_last_pos"
          )
        end
    else
      require(
        $payload.response_row_count == null and
        $payload.response_distinct_audio_url_count == null and
        $payload.response_last_pos == null and
        $payload.response_last_pos_state == "not_observed";
        "unobserved poll retains provider response evidence"
      )
    end;

def is_bootstrap_qualifier:
  .jsonPayload as $payload
  | $payload.outcome == "success" and
    $payload.provider_observed == true and
    $payload.response_last_pos_state == "valid" and
    ($payload.response_last_pos | is_nonnegative_integer) and
    ($payload.request_pos == null or
     $payload.response_last_pos >= $payload.request_pos);

($EXPECTED_SIDS
 | require(type == "array"; "EXPECTED_SIDS must be an array")
 | require(length == 19; "EXPECTED_SIDS must contain exactly 19 identities")
 | require(all(.[]; is_nonempty_string); "EXPECTED_SIDS contains a bad SID")
 | require(. == (sort | unique); "EXPECTED_SIDS must be sorted and unique"))
as $expected_sids
| . as $document
| validate_collection($document; ["bcfy_calls_sid_poll"])
| $document.entries as $entries
| require(
    all($entries[]; validate_poll_entry);
    "poll validation failed"
  )
| require(
    ($entries | map(.jsonPayload.sid) | unique) == $expected_sids;
    "raw poll SID set does not exactly match EXPECTED_SIDS"
  )
| require(
    ($entries
     | group_by([.jsonPayload.sid, .timestamp, .insertId])
     | all(.[]; length == 1));
    "duplicate (sid,timestamp,insertId) event identity"
  )
| ($entries
   | map(
       select(is_bootstrap_qualifier)
       | . + {_epoch: timestamp_epoch(.timestamp; "qualifying timestamp")}
     )
   | sort_by(.jsonPayload.sid, ._epoch, .insertId)) as $qualifying
| require(
    ($qualifying | map(.jsonPayload.sid) | unique) == $expected_sids;
    "qualifying bootstrap SID set does not exactly match EXPECTED_SIDS"
  )
| ($qualifying
   | group_by(.jsonPayload.sid)
   | map(.[0])
   | sort_by(.jsonPayload.sid)) as $retained
| ($retained | map(._epoch) | max) as $bootstrap_end_epoch
| ($retained
   | map(select(._epoch == $bootstrap_end_epoch))
   | sort_by(._epoch, .insertId)
   | last
   | .timestamp) as $bootstrap_end
| {
    status: "pass",
    reducer: "bcfy_calls_sid_bootstrap_v1",
    expected_sid_count: ($expected_sids | length),
    observed_sid_count: ($retained | length),
    expected_sids: $expected_sids,
    retained_first_events: (
      $retained
      | map({
          sid: .jsonPayload.sid,
          timestamp,
          insert_id: .insertId,
          instance_id: .resource.labels.instance_id,
          response_last_pos: .jsonPayload.response_last_pos
        })
    ),
    BOOTSTRAP_END: $bootstrap_end,
    authority_evidence: {
      scope: "sid_poll_events_only",
      configured_authority_evaluated: false,
      direct_wire_selector_absence_evaluated: false
    }
  }
