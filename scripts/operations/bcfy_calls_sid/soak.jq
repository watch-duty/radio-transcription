# Read-only reducer for the exact Broadcastify Calls SID steady-state soak.
#
# Required invocation variables:
#   EXPECTED_SIDS                         sorted JSON array (exactly 19)
#   BOOTSTRAP_END, SOAK_START, SOAK_END   UTC RFC3339 strings
#   MISSING_SUCCESS_THRESHOLD_SECONDS     positive JSON number
#   CURSOR_LAG_THRESHOLD_SECONDS          nonnegative JSON number
#   SUSTAINED_CONSECUTIVE_POLLS           positive JSON integer
#   SUSTAINED_SECONDS                     positive JSON number
#
# The input is one object with complete collection metadata and raw Cloud
# Logging entries.  This reducer does not collect logs or mutate any service.

def fail($message):
  error("bcfy_calls_sid soak evidence rejected: \($message)");

def require($condition; $message):
  if $condition then . else fail($message) end;

def reject_report($message; $aggregate_context; $drill_down):
  {
    status: "fail",
    reducer: "bcfy_calls_sid_soak_v1",
    reason: $message,
    aggregate_context: $aggregate_context,
    drill_down: $drill_down
  }
  | halt_error(5);

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

def validate_raw_entry(
  $entry;
  $event_types;
  $frozen_instances;
  $start;
  $end
):
  $entry
  | require(type == "object"; "every entry must be an object")
  | require(
      .insertId | is_nonempty_string;
      "every entry needs a nonempty insertId"
    )
  | (timestamp_epoch(.timestamp; "entry.timestamp")) as $entry_time
  | require(
      $entry_time >= $start and $entry_time < $end;
      "an entry is outside the exact half-open soak window"
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
      ($event_types | index($entry.jsonPayload.event_type)) != null;
      "soak input contains an unexpected event type"
    )
  | true;

def validate_collection($document; $event_types; $start; $end):
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
  | require(
      $collection.window_start == $SOAK_START and
      $collection.window_end == $SOAK_END;
      "collection metadata does not name the exact soak window"
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
  | require(
      all(
        .entries[];
        validate_raw_entry(
          .;
          $event_types;
          $frozen_instances;
          $start;
          $end
        )
      );
      "raw soak entry validation failed"
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
    end
  | require(
      $payload | has("lease_cursor_lag_seconds") and
      has("maximum_feed_cursor_lag_seconds") and
      has("null_feed_cursor_count");
      "cursor-lag evidence keys are missing"
    )
  | require(
      $payload.lease_cursor_lag_seconds == null or
      ($payload.lease_cursor_lag_seconds | is_nonnegative_number);
      "lease_cursor_lag_seconds is malformed"
    )
  | require(
      $payload.maximum_feed_cursor_lag_seconds == null or
      ($payload.maximum_feed_cursor_lag_seconds | is_nonnegative_number);
      "maximum_feed_cursor_lag_seconds is malformed"
    )
  | require(
      $payload.null_feed_cursor_count | is_nonnegative_integer;
      "null_feed_cursor_count is malformed"
    )
  | require(
      ($payload.pressure_encountered | type) == "boolean";
      "pressure_encountered is malformed"
    )
  | require(
      $payload.pressure_wait_count | is_nonnegative_integer;
      "pressure_wait_count is malformed"
    )
  | require(
      $payload.pressure_encountered == ($payload.pressure_wait_count > 0);
      "pressure flag and wait count disagree"
    )
  | require(
      $payload.oldest_queue_age_seconds | is_nonnegative_number;
      "oldest_queue_age_seconds is malformed"
    )
  | require(
      $payload.maximum_queue_wait_seconds | is_nonnegative_number;
      "maximum_queue_wait_seconds is malformed"
    )
  | require(
      $payload.maximum_queue_depth | is_nonnegative_integer;
      "maximum_queue_depth is malformed"
    )
  | require(
      $payload.maximum_worker_utilization_numerator |
      is_nonnegative_integer;
      "worker utilization numerator is malformed"
    )
  | require(
      $payload.worker_utilization_denominator | is_nonnegative_integer;
      "worker utilization denominator is malformed"
    )
  | require(
      $payload.maximum_worker_utilization_numerator <=
      $payload.worker_utilization_denominator;
      "worker utilization exceeds its denominator"
    )
  | require(
      $payload.fence_rejection_count | is_nonnegative_integer;
      "fence_rejection_count is malformed"
    )
  | require(
      $payload.membership_rejection_count | is_nonnegative_integer;
      "membership_rejection_count is malformed"
    );

def validate_replay_entry:
  .jsonPayload as $payload
  | require(
      $payload.schema_version == 1;
      "replay schema_version must be 1"
    )
  | require($payload.sid | is_nonempty_string; "replay sid is malformed")
  | require(
      $payload.source_type == "bcfy_calls";
      "replay source_type is malformed"
    )
  | require(
      $payload.owner_worker_id | is_nonempty_string;
      "replay owner_worker_id is malformed"
    )
  | require(
      $payload.fencing_token | is_nonnegative_integer;
      "replay fencing_token is malformed"
    )
  | require(
      $payload.cause | is_nonempty_string;
      "replay cause is malformed"
    );

def is_successful_poll:
  .jsonPayload as $payload
  | $payload.outcome == "success" and
    $payload.provider_observed == true and
    $payload.response_last_pos_state == "valid" and
    ($payload.response_last_pos | is_nonnegative_integer) and
    ($payload.request_pos == null or
     $payload.response_last_pos >= $payload.request_pos);

def annotate_reacquisitions:
  group_by(.jsonPayload.sid)
  | map(
      reduce .[] as $entry (
        {prior_generation: null, prior_token: null, rows: []};
        ($entry.jsonPayload |
         [.fencing_token, .owner_worker_id]) as $generation
        | . as $state
        | ($state.prior_generation != null and
           $state.prior_generation != $generation) as $changed
        | ($changed and
           $entry.jsonPayload.fencing_token <= $state.prior_token)
          as $regressed
        | .rows += [
            ($entry + {
              _generation: $generation,
              _reacquisition: $changed,
              _generation_regression: $regressed
            })
          ]
        | .prior_generation = $generation
        | .prior_token = $entry.jsonPayload.fencing_token
      )
    )
  | map(.rows)
  | add
  | sort_by(.jsonPayload.sid, ._epoch, .insertId);

def annotate_hazards($cursor_lag_threshold):
  map(
    . + {
      _hazards: [
        (if .jsonPayload.pressure_encountered then
           "pressure"
         else empty end),
        (if .jsonPayload.fence_rejection_count > 0 then
           "fence_rejection"
         else empty end),
        (if .jsonPayload.membership_rejection_count > 0 then
           "membership_rejection"
         else empty end),
        (if (
          .jsonPayload.lease_cursor_lag_seconds == null or
          .jsonPayload.maximum_feed_cursor_lag_seconds == null or
          .jsonPayload.lease_cursor_lag_seconds > $cursor_lag_threshold or
          .jsonPayload.maximum_feed_cursor_lag_seconds >
          $cursor_lag_threshold or
          .jsonPayload.null_feed_cursor_count > 0
        ) then "cursor_lag" else empty end),
        (if (
          .jsonPayload.provider_observed and
          (.jsonPayload.response_last_pos_state != "valid" or
           (.jsonPayload.request_pos != null and
            .jsonPayload.response_last_pos < .jsonPayload.request_pos))
        ) then "invalid_last_pos" else empty end),
        (if ._reacquisition then "lease_reacquisition" else empty end)
      ]
    }
  );

def hazard_runs($rows; $hazard):
  reduce $rows[] as $row (
    {active: null, completed: []};
    if ($row._hazards | index($hazard)) != null then
      if .active == null then
        .active = {
          sid: $row.jsonPayload.sid,
          hazard: $hazard,
          consecutive_poll_count: 1,
          start_timestamp: $row.timestamp,
          end_timestamp: $row.timestamp,
          duration_seconds: 0,
          first_insert_id: $row.insertId,
          last_insert_id: $row.insertId,
          _start_epoch: $row._epoch
        }
      else
        .active.consecutive_poll_count += 1
        | .active.end_timestamp = $row.timestamp
        | .active.duration_seconds = ($row._epoch - .active._start_epoch)
        | .active.last_insert_id = $row.insertId
      end
    else
      if .active == null then .
      else
        .completed += [(.active | del(._start_epoch))]
        | .active = null
      end
    end
  )
  | if .active == null then .completed
    else .completed + [(.active | del(._start_epoch))]
    end;

def missing_success_violations($rows; $start; $end; $threshold):
  ($rows | map(select(is_successful_poll)) |
   sort_by(._epoch, .insertId)) as $successes
  | if ($successes | length) == 0 then
      [{
        sid: $rows[0].jsonPayload.sid,
        gap_kind: "entire_window",
        gap_seconds: ($end - $start),
        previous_timestamp: $SOAK_START,
        next_timestamp: $SOAK_END
      }]
      | map(select(.gap_seconds > $threshold))
    else
      ([{
          sid: $rows[0].jsonPayload.sid,
          gap_kind: "window_start_to_first_success",
          gap_seconds: ($successes[0]._epoch - $start),
          previous_timestamp: $SOAK_START,
          next_timestamp: $successes[0].timestamp
        }]
       + [
           range(1; $successes | length) as $index
           | {
               sid: $rows[0].jsonPayload.sid,
               gap_kind: "between_successes",
               gap_seconds: (
                 $successes[$index]._epoch -
                 $successes[$index - 1]._epoch
               ),
               previous_timestamp: $successes[$index - 1].timestamp,
               next_timestamp: $successes[$index].timestamp
             }
         ]
       + [{
           sid: $rows[0].jsonPayload.sid,
           gap_kind: "last_success_to_window_end",
           gap_seconds: ($end - ($successes | last | ._epoch)),
           previous_timestamp: ($successes | last | .timestamp),
           next_timestamp: $SOAK_END
         }])
      | map(select(.gap_seconds > $threshold))
    end;

($EXPECTED_SIDS
 | require(type == "array"; "EXPECTED_SIDS must be an array")
 | require(length == 19; "EXPECTED_SIDS must contain exactly 19 identities")
 | require(all(.[]; is_nonempty_string); "EXPECTED_SIDS contains a bad SID")
 | require(. == (sort | unique); "EXPECTED_SIDS must be sorted and unique"))
as $expected_sids
| (timestamp_epoch($BOOTSTRAP_END; "BOOTSTRAP_END")) as $bootstrap_end
| (timestamp_epoch($SOAK_START; "SOAK_START")) as $soak_start
| (timestamp_epoch($SOAK_END; "SOAK_END")) as $soak_end
| require(
    $soak_start > $bootstrap_end;
    "SOAK_START must be strictly later than BOOTSTRAP_END"
  )
| require(
    ($soak_end - $soak_start) == 1800;
    "SOAK_END must be exactly 30 minutes after SOAK_START"
  )
| require(
    $MISSING_SUCCESS_THRESHOLD_SECONDS | is_nonnegative_number and . > 0;
    "MISSING_SUCCESS_THRESHOLD_SECONDS must be a positive number"
  )
| require(
    $CURSOR_LAG_THRESHOLD_SECONDS | is_nonnegative_number;
    "CURSOR_LAG_THRESHOLD_SECONDS must be a nonnegative number"
  )
| require(
    $SUSTAINED_CONSECUTIVE_POLLS | is_integer and . > 0;
    "SUSTAINED_CONSECUTIVE_POLLS must be a positive integer"
  )
| require(
    $SUSTAINED_SECONDS | is_nonnegative_number and . > 0;
    "SUSTAINED_SECONDS must be a positive number"
  )
| . as $document
| validate_collection(
    $document;
    ["bcfy_calls_replay_window_truncated", "bcfy_calls_sid_poll"];
    $soak_start;
    $soak_end
  )
| require(
    ($document.entries
     | group_by([.jsonPayload.sid, .timestamp, .insertId])
     | all(.[]; length == 1));
    "duplicate (sid,timestamp,insertId) event identity"
  )
| ($document.entries |
   map(select(.jsonPayload.event_type == "bcfy_calls_sid_poll"))) as $raw_polls
| ($document.entries |
   map(select(
     .jsonPayload.event_type == "bcfy_calls_replay_window_truncated"
   ))) as $replay_events
| require(
    all($raw_polls[]; validate_poll_entry);
    "poll validation failed"
  )
| require(
    all($replay_events[]; validate_replay_entry);
    "replay validation failed"
  )
  | require(
    ($document.entries | map(.jsonPayload.sid) | unique |
     all(.[]; . as $sid | ($expected_sids | index($sid)) != null));
    "input contains an unexpected SID"
  )
| require(
    ($raw_polls | map(.jsonPayload.sid) | unique) == $expected_sids;
    "settled-poll SID set does not exactly match EXPECTED_SIDS"
  )
| ($raw_polls | length) as $logical_poll_count
| require(
    $logical_poll_count >= 2700 and $logical_poll_count <= 3600;
    "logical poll cadence is outside 90-120 polls/minute inclusive"
  )
| ($raw_polls | map(.jsonPayload.http_attempt_count) | add) as $http_attempts
| require(
    (4 * $http_attempts) <= (5 * $logical_poll_count);
    "HTTP attempts exceed 1.25 per logical poll"
  )
| ($raw_polls | map(select(.jsonPayload.provider_observed)))
  as $observed_polls
| ($observed_polls | map(.jsonPayload.response_row_count) | add // 0)
  as $response_rows
| ($observed_polls |
   map(.jsonPayload.response_distinct_audio_url_count) | add // 0)
  as $per_response_distinct_urls
| require(
    (4 * $response_rows) <= (5 * $per_response_distinct_urls);
    "response rows exceed 1.25 times the summed per-response distinct URLs"
  )
| {
    logical_poll_count: $logical_poll_count,
    http_attempt_count: $http_attempts,
    response_row_count_sum: $response_rows,
    response_distinct_audio_url_count_sum: $per_response_distinct_urls
  } as $aggregate_context
| ($raw_polls |
   map(. + {_epoch: timestamp_epoch(.timestamp; "poll timestamp")}) |
   sort_by(.jsonPayload.sid, ._epoch, .insertId) |
   annotate_reacquisitions |
   annotate_hazards($CURSOR_LAG_THRESHOLD_SECONDS)) as $polls
| ([
     ($polls | group_by(.jsonPayload.sid))[] as $group
     | ($group |
        map(select(
          .jsonPayload.provider_observed and
          .jsonPayload.response_last_pos_state == "valid"
        )) |
        sort_by(._epoch, .insertId)) as $valid
     | range(1; $valid | length) as $index
     | select(
         $valid[$index].jsonPayload.response_last_pos <
         $valid[$index - 1].jsonPayload.response_last_pos
       )
     | {
         sid: $valid[$index].jsonPayload.sid,
         previous_timestamp: $valid[$index - 1].timestamp,
         previous_insert_id: $valid[$index - 1].insertId,
         previous_last_pos: (
           $valid[$index - 1].jsonPayload.response_last_pos
         ),
         timestamp: $valid[$index].timestamp,
         insert_id: $valid[$index].insertId,
         response_last_pos: $valid[$index].jsonPayload.response_last_pos
       }
   ]) as $last_pos_regressions
| if ($last_pos_regressions | length) == 0 then .
  else reject_report(
    "response_last_pos regressed within a SID";
    $aggregate_context;
    {
      total_count: ($last_pos_regressions | length),
      retained_limit: 100,
      last_pos_regressions: $last_pos_regressions[:100],
      truncated: (($last_pos_regressions | length) > 100)
    }
  ) end
| ($polls | map(select(._generation_regression))) as $generation_regressions
| if ($generation_regressions | length) == 0 then .
  else reject_report(
    "a Lease generation changed without a strictly increasing fence";
    $aggregate_context;
    {
      total_count: ($generation_regressions | length),
      retained_limit: 100,
      generation_regressions: (
        $generation_regressions[:100]
        | map({
            sid: .jsonPayload.sid,
            timestamp,
            insert_id: .insertId,
            fencing_token: .jsonPayload.fencing_token,
            owner_worker_id: .jsonPayload.owner_worker_id
          })
      ),
      truncated: (($generation_regressions | length) > 100)
    }
  ) end
| ([
     ($polls | group_by(.jsonPayload.sid))[] as $group
     | missing_success_violations(
         $group;
         $soak_start;
         $soak_end;
         $MISSING_SUCCESS_THRESHOLD_SECONDS
       )[]
   ]) as $missing_success
| if ($missing_success | length) == 0 then .
  else reject_report(
    "a SID exceeds MISSING_SUCCESS_THRESHOLD_SECONDS, including window edges";
    $aggregate_context;
    {
      total_count: ($missing_success | length),
      retained_limit: 100,
      missing_success_gaps: $missing_success[:100],
      truncated: (($missing_success | length) > 100)
    }
  ) end
| ([
     ($polls | group_by(.jsonPayload.sid))[] as $group
     | [
         "pressure",
         "fence_rejection",
         "membership_rejection",
         "cursor_lag",
         "invalid_last_pos",
         "lease_reacquisition"
       ][] as $hazard
     | (hazard_runs($group; $hazard))[]
     | select(
         .consecutive_poll_count >= $SUSTAINED_CONSECUTIVE_POLLS or
         .duration_seconds >= $SUSTAINED_SECONDS
       )
   ]) as $sustained_hazards
| if ($sustained_hazards | length) == 0 then .
  else reject_report(
    "a sustained SID hazard breached the configured poll-count/time gate";
    $aggregate_context;
    {
      total_count: ($sustained_hazards | length),
      retained_limit: 100,
      sustained_hazards: $sustained_hazards[:100],
      truncated: (($sustained_hazards | length) > 100)
    }
  ) end
| if ($replay_events | length) == 0 then .
  else reject_report(
    "replay-window truncation evidence is present";
    $aggregate_context;
    {
      total_count: ($replay_events | length),
      retained_limit: 100,
      replay_window_truncations: (
        $replay_events[:100]
        | map({
            sid: .jsonPayload.sid,
            timestamp,
            insert_id: .insertId,
            fencing_token: .jsonPayload.fencing_token,
            owner_worker_id: .jsonPayload.owner_worker_id,
            cause: .jsonPayload.cause
          })
      ),
      truncated: (($replay_events | length) > 100)
    }
  ) end
| ($polls | map(select((._hazards | length) > 0))) as $hazard_events
| {
    status: "pass",
    reducer: "bcfy_calls_sid_soak_v1",
    window: {
      BOOTSTRAP_END: $BOOTSTRAP_END,
      SOAK_START: $SOAK_START,
      SOAK_END: $SOAK_END,
      half_open_minutes: 30
    },
    configured_thresholds: {
      missing_success_seconds: $MISSING_SUCCESS_THRESHOLD_SECONDS,
      cursor_lag_seconds: $CURSOR_LAG_THRESHOLD_SECONDS,
      sustained_consecutive_polls: $SUSTAINED_CONSECUTIVE_POLLS,
      sustained_seconds: $SUSTAINED_SECONDS
    },
    gates: {
      exact_sid_set: true,
      expected_sid_count: ($expected_sids | length),
      logical_poll_count: $logical_poll_count,
      logical_polls_per_minute: ($logical_poll_count / 30),
      http_attempt_count: $http_attempts,
      attempts_per_logical_poll: ($http_attempts / $logical_poll_count),
      response_row_count_sum: $response_rows,
      response_distinct_audio_url_count_sum: $per_response_distinct_urls,
      response_rows_per_summed_per_response_distinct_url: (
        if $per_response_distinct_urls == 0 then null
        else $response_rows / $per_response_distinct_urls
        end
      ),
      zero_over_zero_row_amplification_passed: (
        $response_rows == 0 and $per_response_distinct_urls == 0
      ),
      last_pos_monotonic: true,
      missing_success_gap_count: 0,
      sustained_hazard_count: 0,
      replay_window_truncation_count: 0,
      maximum_oldest_queue_age_seconds: (
        $polls | map(.jsonPayload.oldest_queue_age_seconds) | max
      ),
      maximum_queue_wait_seconds: (
        $polls | map(.jsonPayload.maximum_queue_wait_seconds) | max
      ),
      maximum_queue_depth: (
        $polls | map(.jsonPayload.maximum_queue_depth) | max
      ),
      maximum_worker_utilization: (
        $polls
        | map(
            select(.jsonPayload.worker_utilization_denominator > 0)
            | (.jsonPayload.maximum_worker_utilization_numerator /
               .jsonPayload.worker_utilization_denominator)
          )
        | max // 0
      )
    },
    denominator_interpretation: (
      "sum_of_exact_per_response_distinct_audio_url_counts"
    ),
    window_wide_cross_poll_uniqueness_evaluated: false,
    authority_evidence: {
      scope: "sid_poll_and_replay_events_only",
      configured_authority_evaluated: false,
      direct_wire_selector_absence_evaluated: false
    },
    drill_down: {
      hazard_event_count: ($hazard_events | length),
      retained_event_limit: 100,
      retained_events: (
        $hazard_events[:100]
        | map({
            sid: .jsonPayload.sid,
            timestamp,
            insert_id: .insertId,
            hazards: ._hazards,
            fencing_token: .jsonPayload.fencing_token,
            owner_worker_id: .jsonPayload.owner_worker_id,
            lease_cursor_lag_seconds: (
              .jsonPayload.lease_cursor_lag_seconds
            ),
            maximum_feed_cursor_lag_seconds: (
              .jsonPayload.maximum_feed_cursor_lag_seconds
            ),
            pressure_encountered: .jsonPayload.pressure_encountered,
            oldest_queue_age_seconds: (
              .jsonPayload.oldest_queue_age_seconds
            ),
            maximum_queue_wait_seconds: (
              .jsonPayload.maximum_queue_wait_seconds
            ),
            maximum_queue_depth: .jsonPayload.maximum_queue_depth,
            maximum_worker_utilization_numerator: (
              .jsonPayload.maximum_worker_utilization_numerator
            ),
            worker_utilization_denominator: (
              .jsonPayload.worker_utilization_denominator
            ),
            fence_rejection_count: .jsonPayload.fence_rejection_count,
            membership_rejection_count: (
              .jsonPayload.membership_rejection_count
            )
          })
      ),
      retained_events_truncated: (($hazard_events | length) > 100)
    }
  }
