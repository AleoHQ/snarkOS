// Copyright (C) 2019-2023 Aleo Systems Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::MAX_TIMESTAMP_DELTA_IN_SECS;
use snarkvm::prelude::{bail, Result};

use time::OffsetDateTime;

/// Returns the current UTC epoch timestamp.
pub fn now() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

/// Sanity checks the timestamp for liveness.
pub fn check_timestamp_for_liveness(timestamp: i64, previous_timestamp: i64) -> Result<()> {
    // Ensure the timestamp is within range.
    if timestamp > (now() + MAX_TIMESTAMP_DELTA_IN_SECS) {
        bail!("Timestamp {timestamp} is too far in the future")
    }

    // Ensure the timestamp is after the previous timestamp.
    if timestamp <= previous_timestamp {
        bail!("Timestamp {timestamp} for the proposed batch must be after the previous round timestamp")
    }

    Ok(())
}

#[cfg(test)]
mod prop_tests {
    use super::{check_timestamp_for_liveness, now};
    use crate::MAX_TIMESTAMP_DELTA_IN_SECS;

    use proptest::prelude::*;
    use test_strategy::proptest;

    fn any_valid_timestamp() -> BoxedStrategy<i64> {
        (Just(now()), 0..MAX_TIMESTAMP_DELTA_IN_SECS).prop_map(|(now, delta)| now + delta).boxed()
    }

    fn any_invalid_timestamp() -> BoxedStrategy<i64> {
        (Just(now()), MAX_TIMESTAMP_DELTA_IN_SECS..).prop_map(|(now, delta)| now + delta).boxed()
    }

    fn any_previous_timestamp() -> BoxedStrategy<i64> {
        (0..now()).prop_map(|timestamp| timestamp).boxed()
    }

    #[proptest]
    fn test_check_timestamp_for_liveness(
        #[strategy(any_valid_timestamp())] timestamp: i64,
        #[strategy(any_previous_timestamp())] previous_timestamp: i64,
    ) {
        let result = check_timestamp_for_liveness(timestamp, previous_timestamp);
        assert!(result.is_ok());
    }

    #[proptest]
    fn test_check_timestamp_for_liveness_too_far_in_future(
        #[strategy(any_invalid_timestamp())] timestamp: i64,
        #[strategy(any_previous_timestamp())] previous_timestamp: i64,
    ) {
        let result = check_timestamp_for_liveness(timestamp, previous_timestamp);
        assert!(result.is_err());
    }

    #[proptest]
    fn test_check_timestamp_for_liveness_too_far_in_past(
        #[strategy(any_previous_timestamp())] previous_timestamp: i64,
        #[strategy(0..#previous_timestamp)] timestamp: i64,
    ) {
        let result = check_timestamp_for_liveness(timestamp, previous_timestamp);
        assert!(result.is_err());
    }
}
