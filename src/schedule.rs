//! Session schedules: when a session is active and when it resets, ported
//! from quickfix C++'s `TimeRange`.
//!
//! A [`Schedule`] is a daily window (`StartTime`/`EndTime`) or a weekly one
//! (adding `StartDay`/`EndDay`). [`Schedule::is_in_range`] answers "is now
//! inside the window"; [`Schedule::is_in_same_range`] answers "do these two
//! instants fall in the *same occurrence* of the window" — the test that
//! drives the daily/weekly sequence-number reset.

use chrono::{DateTime, Datelike, Local, Timelike, Utc};

use crate::error::{Error, Result};

const SECONDS_PER_DAY: i64 = 86_400;

/// A time of day as seconds since midnight (`0..86400`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tod(i64);

impl Tod {
    fn parse(s: &str) -> Result<Self> {
        let bad = || Error::Config(format!("invalid time {s:?}, expected HH:MM:SS"));
        let mut parts = s.split(':');
        let h: i64 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
        let m: i64 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
        let sec: i64 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
        if parts.next().is_some() || h > 23 || m > 59 || sec > 59 {
            return Err(bad());
        }
        Ok(Tod(h * 3600 + m * 60 + sec))
    }
}

/// Parse a day abbreviation (first two letters, case-insensitive) to the
/// C++ weekday numbering: Sunday=1 .. Saturday=7.
fn parse_day(s: &str) -> Result<i32> {
    let abbr: String = s.chars().take(2).flat_map(|c| c.to_lowercase()).collect();
    Ok(match abbr.as_str() {
        "su" => 1,
        "mo" => 2,
        "tu" => 3,
        "we" => 4,
        "th" => 5,
        "fr" => 6,
        "sa" => 7,
        _ => return Err(Error::Config(format!("invalid day {s:?}"))),
    })
}

/// Weekday of a date in C++ numbering (Sunday=1 .. Saturday=7).
fn weekday(date: chrono::NaiveDate) -> i32 {
    date.weekday().num_days_from_sunday() as i32 + 1
}

/// A monotonic day count, consistent with [`weekday`] (both advance by one
/// per calendar day), used as the C++ "Julian date" for weekly anchoring.
fn day_number(date: chrono::NaiveDate) -> i32 {
    date.num_days_from_ce()
}

#[derive(Debug, Clone)]
pub struct Schedule {
    start: Tod,
    end: Tod,
    /// -1 when this is a daily (non-weekly) window.
    start_day: i32,
    end_day: i32,
    use_local: bool,
    /// True when the session is 24/7 (either `NonStopSession=Y` or no times
    /// configured, our friendlier default).
    non_stop: bool,
}

impl Default for Schedule {
    /// No schedule configured: always active, never resets.
    fn default() -> Self {
        Self {
            start: Tod(0),
            end: Tod(0),
            start_day: -1,
            end_day: -1,
            use_local: false,
            non_stop: true,
        }
    }
}

impl Schedule {
    pub fn non_stop() -> Self {
        Self::default()
    }

    /// Build from raw config strings. Any of `start`/`end` unset yields a
    /// non-stop schedule (unless days are given). Returns a config error on
    /// malformed times/days or a StartDay-without-EndDay mismatch.
    pub fn parse(
        start: Option<&str>,
        end: Option<&str>,
        start_day: Option<&str>,
        end_day: Option<&str>,
        use_local: bool,
        non_stop_setting: bool,
    ) -> Result<Self> {
        let start_day = start_day.map(parse_day).transpose()?.unwrap_or(-1);
        let end_day = end_day.map(parse_day).transpose()?.unwrap_or(-1);
        if (start_day >= 0) != (end_day >= 0) {
            return Err(Error::Config("StartDay and EndDay must be set together".into()));
        }
        match (start, end) {
            (Some(s), Some(e)) => Ok(Self {
                start: Tod::parse(s)?,
                end: Tod::parse(e)?,
                start_day,
                end_day,
                use_local,
                non_stop: false,
            }),
            _ if non_stop_setting || start_day < 0 => Ok(Self::non_stop()),
            // Days set but no times: default to midnight-to-midnight (a full
            // weekly window).
            _ => Ok(Self {
                start: Tod(0),
                end: Tod(0),
                start_day,
                end_day,
                use_local,
                non_stop: false,
            }),
        }
    }

    pub fn is_non_stop(&self) -> bool {
        self.non_stop
    }

    /// (time-of-day seconds, weekday 1..7, day number) in the schedule's zone.
    fn parts(&self, now: DateTime<Utc>) -> (i64, i32, i32) {
        if self.use_local {
            let l = now.with_timezone(&Local);
            (
                l.time().num_seconds_from_midnight() as i64,
                weekday(l.date_naive()),
                day_number(l.date_naive()),
            )
        } else {
            (
                now.time().num_seconds_from_midnight() as i64,
                weekday(now.date_naive()),
                day_number(now.date_naive()),
            )
        }
    }

    /// Is `now` within the active window?
    pub fn is_in_range(&self, now: DateTime<Utc>) -> bool {
        if self.non_stop {
            return true;
        }
        let (tod, wd, _) = self.parts(now);
        if self.start_day < 0 {
            in_daily(self.start, self.end, tod)
        } else {
            self.in_weekly(tod, wd)
        }
    }

    fn in_weekly(&self, tod: i64, day: i32) -> bool {
        let (sd, ed) = (self.start_day, self.end_day);
        if sd == ed {
            if day != sd {
                return true;
            }
            return in_daily(self.start, self.end, tod);
        } else if sd < ed {
            if day < sd || day > ed {
                return false;
            }
        } else {
            // sd > ed: window wraps across the week boundary.
            if day < sd && day > ed {
                return false;
            }
        }
        if day == sd && tod < self.start.0 {
            return false;
        }
        if day == ed && tod > self.end.0 {
            return false;
        }
        true
    }

    /// Do `now` and `other` fall in the same occurrence of the window? When
    /// this is false, the session has crossed into a new instance and its
    /// sequence numbers should reset.
    pub fn is_in_same_range(&self, now: DateTime<Utc>, other: DateTime<Utc>) -> bool {
        if self.non_stop {
            return true;
        }
        if !self.is_in_range(now) || !self.is_in_range(other) {
            return false;
        }
        if now == other {
            return true;
        }
        if self.start_day < 0 {
            self.same_daily(now, other)
        } else if self.start_day != self.end_day {
            self.range_start_date(now) == self.range_start_date(other)
        } else {
            self.same_weekly_sameday(now, other)
        }
    }

    fn same_daily(&self, t1: DateTime<Utc>, t2: DateTime<Utc>) -> bool {
        if self.start.0 <= self.end.0 {
            // Non-overnight (incl. start == end = 24h): same calendar date.
            self.parts(t1).2 == self.parts(t2).2
        } else {
            // Overnight wrap: within one contiguous session span.
            let session_length = SECONDS_PER_DAY - (self.start.0 - self.end.0);
            let diff = (t1 - t2).num_seconds();
            if diff > 0 {
                let t2_tod = self.parts(t2).0;
                let mut delta = t2_tod - self.start.0;
                if delta < 0 {
                    delta = SECONDS_PER_DAY - delta.abs();
                }
                diff < (session_length - delta)
            } else {
                -diff < session_length
            }
        }
    }

    /// The day-number of the current weekly window's start (C++
    /// `getRangeStartDate`): the most recent `start_day` at `start`.
    fn range_start_date(&self, now: DateTime<Utc>) -> i32 {
        let (tod, wd, jul) = self.parts(now);
        let sd = self.start_day;
        if wd > sd {
            jul - wd + sd
        } else if wd < sd {
            jul - wd + sd - 7
        } else if tod >= self.start.0 {
            jul
        } else {
            jul - 7
        }
    }

    fn same_weekly_sameday(&self, t1: DateTime<Utc>, t2: DateTime<Utc>) -> bool {
        let (tod1, wd1, day1) = self.parts(t1);
        let (tod2, _wd2, day2) = self.parts(t2);
        let sd = self.start_day;
        if day1 == day2 && sd == wd1 {
            let both_before_end = tod1 <= self.end.0 && tod2 <= self.end.0;
            let both_after_start = tod1 >= self.start.0 && tod2 >= self.start.0;
            both_before_end || both_after_start
        } else if day1 == day2 && sd != wd1 {
            true
        } else if (day1 - day2).abs() > 7 {
            false
        } else if (day1 - day2).abs() == 7 {
            if wd1 != sd {
                return false;
            }
            let (earlier_tod, later_tod) =
                if day2 > day1 { (tod1, tod2) } else { (tod2, tod1) };
            earlier_tod >= self.start.0 && later_tod <= self.end.0
        } else {
            self.range_start_date(t1) == self.range_start_date(t2)
        }
    }
}

fn in_daily(start: Tod, end: Tod, tod: i64) -> bool {
    if start.0 < end.0 {
        tod >= start.0 && tod <= end.0
    } else {
        // start > end (overnight) or start == end (always active).
        tod >= start.0 || tod <= end.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
    }

    fn daily(start: &str, end: &str) -> Schedule {
        Schedule::parse(Some(start), Some(end), None, None, false, false).unwrap()
    }

    #[test]
    fn parse_days_and_times() {
        assert!(Tod::parse("00:00:00").is_ok());
        assert!(Tod::parse("23:59:59").is_ok());
        assert!(Tod::parse("24:00:00").is_err());
        assert!(Tod::parse("12:60:00").is_err());
        assert!(Tod::parse("12:00").is_err());
        assert_eq!(parse_day("Sunday").unwrap(), 1);
        assert_eq!(parse_day("SA").unwrap(), 7);
        assert_eq!(parse_day("we").unwrap(), 4);
        assert!(parse_day("xy").is_err());
    }

    #[test]
    fn weekday_numbering_matches_cpp() {
        // 2024-05-19 is a Sunday.
        assert_eq!(weekday(chrono::NaiveDate::from_ymd_opt(2024, 5, 19).unwrap()), 1);
        assert_eq!(weekday(chrono::NaiveDate::from_ymd_opt(2024, 5, 25).unwrap()), 7); // Saturday
    }

    #[test]
    fn daytime_window() {
        let s = daily("08:00:00", "17:00:00");
        assert!(!s.is_in_range(utc(2024, 5, 20, 7, 59, 59)));
        assert!(s.is_in_range(utc(2024, 5, 20, 8, 0, 0)));
        assert!(s.is_in_range(utc(2024, 5, 20, 12, 0, 0)));
        assert!(s.is_in_range(utc(2024, 5, 20, 17, 0, 0)));
        assert!(!s.is_in_range(utc(2024, 5, 20, 17, 0, 1)));
    }

    #[test]
    fn daily_reset_same_and_different_day() {
        let s = daily("08:00:00", "17:00:00");
        // Same day, both in window -> same range.
        assert!(s.is_in_same_range(utc(2024, 5, 20, 9, 0, 0), utc(2024, 5, 20, 16, 0, 0)));
        // Next day -> different range (a reset boundary was crossed).
        assert!(!s.is_in_same_range(utc(2024, 5, 21, 9, 0, 0), utc(2024, 5, 20, 16, 0, 0)));
    }

    #[test]
    fn always_on_window_resets_at_midnight() {
        // StartTime == EndTime: 24h window that resets once per calendar day.
        let s = daily("00:00:00", "00:00:00");
        assert!(s.is_in_range(utc(2024, 5, 20, 3, 0, 0)));
        assert!(s.is_in_same_range(utc(2024, 5, 20, 3, 0, 0), utc(2024, 5, 20, 22, 0, 0)));
        assert!(!s.is_in_same_range(utc(2024, 5, 21, 1, 0, 0), utc(2024, 5, 20, 22, 0, 0)));
    }

    #[test]
    fn overnight_window() {
        // 17:00 -> 08:00 next day.
        let s = daily("17:00:00", "08:00:00");
        assert!(s.is_in_range(utc(2024, 5, 20, 23, 0, 0)));
        assert!(s.is_in_range(utc(2024, 5, 21, 3, 0, 0)));
        assert!(!s.is_in_range(utc(2024, 5, 20, 12, 0, 0)));
        // Evening and the following early morning are the SAME session.
        assert!(s.is_in_same_range(utc(2024, 5, 20, 23, 0, 0), utc(2024, 5, 21, 3, 0, 0)));
        // Two nights apart -> different sessions.
        assert!(!s.is_in_same_range(utc(2024, 5, 20, 23, 0, 0), utc(2024, 5, 21, 23, 30, 0)));
    }

    #[test]
    fn weekly_window() {
        // Sunday 00:00 -> Friday 17:00 (a classic FX week).
        let s = Schedule::parse(
            Some("00:00:00"),
            Some("17:00:00"),
            Some("Sunday"),
            Some("Friday"),
            false,
            false,
        )
        .unwrap();
        // 2024-05-19 Sun .. 2024-05-24 Fri.
        assert!(s.is_in_range(utc(2024, 5, 19, 1, 0, 0))); // Sunday
        assert!(s.is_in_range(utc(2024, 5, 22, 12, 0, 0))); // Wednesday
        assert!(s.is_in_range(utc(2024, 5, 24, 17, 0, 0))); // Friday 17:00
        assert!(!s.is_in_range(utc(2024, 5, 24, 17, 0, 1))); // just after close
        assert!(!s.is_in_range(utc(2024, 5, 25, 12, 0, 0))); // Saturday
        // Wed and Fri of the same week -> same session.
        assert!(s.is_in_same_range(utc(2024, 5, 22, 12, 0, 0), utc(2024, 5, 24, 12, 0, 0)));
        // Across the weekend into the next week -> different session.
        assert!(!s.is_in_same_range(utc(2024, 5, 24, 12, 0, 0), utc(2024, 5, 27, 12, 0, 0)));
    }

    #[test]
    fn non_stop_never_resets() {
        let s = Schedule::non_stop();
        assert!(s.is_in_range(utc(2024, 5, 20, 3, 0, 0)));
        assert!(s.is_in_same_range(utc(2024, 1, 1, 0, 0, 0), utc(2024, 12, 31, 23, 59, 59)));
    }
}
