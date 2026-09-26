// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Cron trigger schedules, reified sans-IO.
//!
//! A `triggers.crons` entry in the Wrangler config becomes one reserved cell
//! whose alarm is armed at the next occurrence. Everything about *when* that
//! is — parsing the expression, walking to the next minute, choosing between
//! a retry and the next occurrence — is a pure function of the expression and
//! a timestamp. The cell and its alarm row are the executor.
//!
//! Resolution is one minute and the zone is UTC, matching both Cloudflare's
//! cron triggers and the minute buckets the wake index already uses
//! (`wake::entry_key`). The schedule is therefore never more precise than the
//! alarm that carries it.
//!
//! The dialect is Cloudflare's, read out of `saffron` (the parser behind Cron
//! Triggers) rather than out of a POSIX cron manual, because the two disagree
//! on the day-of-week numbers and silence is the worst way to find that out.
//! Cloudflare numbers the weekdays 1 to 7 from Sunday and rejects 0, so `1-5`
//! is Sunday to Thursday there and Monday to Friday in Vixie cron. Two places
//! where `saffron` contradicts its own comments are called out at the code that
//! declines to copy them.

use crate::wake::civil_from_days;
use crate::Ms;

/// The class name of the reserved cell that carries a script's schedule.
///
/// A leading `.` is inside `cell::valid_cell_scope`'s charset but is not a
/// legal start for a JavaScript identifier, so no exported class can collide
/// with the reserved namespace. That is the whole reason the name is punctuated
/// rather than pretty.
pub const RESERVED_CLASS: &str = ".cron";

/// The one cell that owns a script's cron schedule.
///
/// Keyed on the script and nothing else — not the expressions, not a hash of
/// them. Ownership CAS on this one name is what makes a cron fire once per
/// fleet rather than once per node, and a stable name is what lets a deploy
/// change the schedule without stranding the alarm the old schedule armed.
pub fn reserved_cell(script_name: &str) -> String {
    format!("{RESERVED_CLASS}:{script_name}")
}

/// How far ahead `next_after` walks before it calls an expression
/// unsatisfiable.
///
/// One Gregorian cycle. The calendar repeats exactly every 400 years, so a walk
/// of 146097 consecutive days sees every (month, day-of-month, day-of-week)
/// combination that can ever occur: an expression with no match in that window
/// has none at all, and the bound is a proof rather than an estimate. Estimates
/// were tried and were wrong twice over — `0 0 29 2 *` waits eight years, not
/// four, when a century skips its leap year (2096-02-29 to 2104-02-29 is 2921
/// days), and `0 0 * 2 SUN#5` needs a 29-day February that starts on a Sunday,
/// which is 28 years apart. Undershooting is not a late fire but a silent
/// retirement: `next_across` returns `None`, so the cell deletes its alarm. The
/// full walk costs about a hundred thousand cheap iterations and only an
/// unsatisfiable expression ever pays it, once, at arm time.
const MAX_LOOKAHEAD_DAYS: usize = 146_097;

const MINUTE: Field = Field {
    name: "minute",
    min: 0,
    max: 59,
    names: &[],
};
const HOUR: Field = Field {
    name: "hour",
    min: 0,
    max: 23,
    names: &[],
};
const DAY_OF_MONTH: Field = Field {
    name: "day of month",
    min: 1,
    max: 31,
    names: &[],
};
const MONTH: Field = Field {
    name: "month",
    min: 1,
    max: 12,
    names: &[
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ],
};
/// Cloudflare numbers the weekdays 1 to 7 from Sunday, so 1 is Sunday and 7 is
/// Saturday, and 0 is out of range. Vixie cron numbers them 0 to 6 from Sunday
/// and accepts 7 as Sunday again, which makes the same expression name a
/// different day on the two platforms. Cloudflare documents its choice, and a
/// value here is therefore the Cloudflare number minus one: the bit sets below
/// are indexed by weekday with Sunday at 0.
const DAY_OF_WEEK: Field = Field {
    name: "day of week",
    min: 1,
    max: 7,
    names: &["sun", "mon", "tue", "wed", "thu", "fri", "sat"],
};

/// One field's shape: its bounds and the three-letter aliases it accepts.
struct Field {
    name: &'static str,
    min: u32,
    max: u32,
    /// Aliases in ascending order starting at `min`.
    names: &'static [&'static str],
}

/// A parsed five-field cron expression, as bit sets.
///
/// The original text is kept because the `scheduled` handler's controller
/// reports which expression fired, and it must be the string the developer
/// wrote rather than a re-rendering of these bits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cron {
    text: String,
    minute: u64,
    hour: u32,
    month: u16,
    day_of_month: DayOfMonth,
    day_of_week: DayOfWeek,
}

/// The day-of-month field. `All` is its own variant because the day rule turns
/// on whether the field was written `*`, which a bit set cannot say: a field
/// listing every day is indistinguishable from `*` once expanded. The `L` and
/// `W` variants cannot be expressed as a bit set at all, because which day they
/// name depends on the month.
#[derive(Clone, Debug, PartialEq, Eq)]
enum DayOfMonth {
    /// `*`
    All,
    /// A list, range, or step: bit `d` set for day `d`.
    Days(u32),
    /// `L` — the last day of the month.
    Last,
    /// `L-<n>` — `n` days before the last day.
    LastOffset(u32),
    /// `LW` — the last weekday of the month.
    LastWeekday,
    /// `L-<n>W` — the weekday closest to `n` days before the last day.
    LastOffsetWeekday(u32),
    /// `<d>W` — the weekday closest to day `d`, inside the same month.
    ClosestWeekday(u32),
}

/// The day-of-week field, with Sunday at bit 0 whatever number named it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum DayOfWeek {
    /// `*`
    All,
    /// A list, range, or step: bit `w` set for weekday `w`, Sunday at 0.
    Days(u8),
    /// `<dow>L` — the last such weekday of the month.
    Last(u32),
    /// `<dow>#<n>` — the nth such weekday of the month.
    Nth(u32, u32),
}

impl Cron {
    /// The expression as the developer wrote it.
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Why an expression was refused, naming the field at fault so a deploy can
/// report which of several crons is wrong.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronError {
    pub expression: String,
    pub message: String,
}

impl std::fmt::Display for CronError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cron expression {:?}: {}", self.expression, self.message)
    }
}

impl std::error::Error for CronError {}

/// Parse a five-field cron expression: minute, hour, day of month, month, day
/// of week. Each field takes `*`, a value, an `a-b` range, an `a,b` list, and a
/// `/n` step on any of those. Month and day-of-week also take the usual
/// three-letter names, case-insensitively. The day fields take Cloudflare's
/// `L`, `W`, and `#` on top of that.
///
/// `?` is refused, as Cloudflare refuses it: its parser has no case for the
/// character, so an expression that uses it fails there too.
pub fn parse(expression: &str) -> Result<Cron, CronError> {
    let fail = |message: String| CronError {
        expression: expression.to_string(),
        message,
    };
    let fields: Vec<&str> = expression.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(fail(format!(
            "expected 5 fields (minute hour day-of-month month day-of-week), found {}",
            fields.len()
        )));
    }
    let minute = parse_field(fields[0], &MINUTE).map_err(&fail)?;
    let hour = parse_field(fields[1], &HOUR).map_err(&fail)?;
    let day_of_month = parse_day_of_month(fields[2]).map_err(&fail)?;
    let month = parse_field(fields[3], &MONTH).map_err(&fail)?;
    let day_of_week = parse_day_of_week(fields[4]).map_err(&fail)?;
    Ok(Cron {
        text: expression.trim().to_string(),
        minute,
        hour: hour as u32,
        month: month as u16,
        day_of_month,
        day_of_week,
    })
}

/// The day-of-month field, including the `L` and `W` forms.
///
/// An `L` or `W` form stands alone. Cloudflare's parser stops after reading one
/// and never looks for a comma, so `1W,15` fails there; refusing it here keeps
/// the two answers the same instead of inventing a union it cannot express.
fn parse_day_of_month(text: &str) -> Result<DayOfMonth, String> {
    if text == "*" {
        return Ok(DayOfMonth::All);
    }
    if let Some(rest) = text.strip_prefix('L') {
        return match rest {
            "" => Ok(DayOfMonth::Last),
            "W" => Ok(DayOfMonth::LastWeekday),
            _ => {
                let (digits, weekday) = match rest.strip_suffix('W') {
                    Some(digits) => (digits, true),
                    None => (rest, false),
                };
                let offset = digits.strip_prefix('-').ok_or_else(|| {
                    format!("day of month field has an unreadable `L` form in {text:?}")
                })?;
                // Cloudflare allows 1 to 30: 30 is one short of the longest
                // month, so `L-30` is the 1st of a 31-day month, and 0 is out
                // of range rather than a spelling of `L`. Accepting 0 would
                // also make `L-0W` a different rule from `LW`, because the
                // offset arms cannot move to the Friday before a month that
                // ends on a weekend.
                let offset = parse_digits(offset).ok_or_else(|| {
                    format!("day of month field has a non-numeric `L` offset in {text:?}")
                })?;
                if !(1..=30).contains(&offset) {
                    return Err(format!(
                        "day of month field has an `L` offset {offset} outside 1-30"
                    ));
                }
                if weekday {
                    Ok(DayOfMonth::LastOffsetWeekday(offset))
                } else {
                    Ok(DayOfMonth::LastOffset(offset))
                }
            }
        };
    }
    if let Some(day) = text.strip_suffix('W') {
        return Ok(DayOfMonth::ClosestWeekday(parse_value(day, &DAY_OF_MONTH)?));
    }
    Ok(DayOfMonth::Days(parse_field(text, &DAY_OF_MONTH)? as u32))
}

/// The day-of-week field, including the `L` and `#` forms.
///
/// `L` on its own is Saturday, which reads like a typo and is not: Cloudflare's
/// parser maps a bare `L` in this field to day 7, and 7 is Saturday under its
/// numbering.
fn parse_day_of_week(text: &str) -> Result<DayOfWeek, String> {
    if text == "*" {
        return Ok(DayOfWeek::All);
    }
    if text == "L" {
        return Ok(DayOfWeek::Days(1 << 6));
    }
    if let Some(day) = text.strip_suffix('L') {
        return Ok(DayOfWeek::Last(parse_value(day, &DAY_OF_WEEK)? - 1));
    }
    if let Some((day, nth)) = text.split_once('#') {
        let day = parse_value(day, &DAY_OF_WEEK)? - 1;
        let nth = parse_digits(nth)
            .ok_or_else(|| format!("day of week field has a non-numeric `#` count in {text:?}"))?;
        if !(1..=5).contains(&nth) {
            return Err(format!(
                "day of week field has a `#` count {nth} outside 1-5"
            ));
        }
        return Ok(DayOfWeek::Nth(day, nth));
    }
    // Parsed in Cloudflare's 1-7 numbering and shifted down, so a range keeps
    // its meaning: `2-6` is bits 2 to 6 before the shift and Monday to Friday
    // after it.
    Ok(DayOfWeek::Days(
        (parse_field(text, &DAY_OF_WEEK)? >> 1) as u8,
    ))
}

/// One field to a bit set, bit `v` set when value `v` matches.
fn parse_field(text: &str, field: &Field) -> Result<u64, String> {
    if text.is_empty() {
        return Err(format!("{} field is empty", field.name));
    }
    let mut bits = 0_u64;
    let items: Vec<&str> = text.split(',').collect();
    for item in &items {
        // A bare `*` inside a list is refused, wherever in the list it sits.
        // Cloudflare rejects `*,5` outright — its parser returns on the first
        // `*` and then fails on the unread remainder — and accepts `1,*` while
        // reading that `*` as the field's minimum, so `1,*` is minute 0 and
        // minute 1 there. Both follow from one parsing routine being reused and
        // not from any intent, and reading either as "every value" would fire a
        // handler sixty times where Cloudflare fires it twice. Neither answer
        // can be defended, so the deploy stops where the developer can read it.
        if items.len() > 1 && item.trim() == "*" {
            return Err(format!(
                "{} field has a `*` inside a list in {text:?}; write `*` alone, or list the values",
                field.name
            ));
        }
        bits |= parse_item(item, field)?;
    }
    Ok(bits)
}

fn parse_item(item: &str, field: &Field) -> Result<u64, String> {
    let (spec, step) = match item.split_once('/') {
        None => (item, 1_u32),
        Some((spec, step)) => {
            let step = parse_digits(step).ok_or_else(|| {
                format!("{} field has a non-numeric step in {item:?}", field.name)
            })?;
            // Cloudflare bounds a step by the width of its field, so `*/60` is
            // refused for minutes rather than read as "every hour". A step at
            // or past the width can only ever set the value it starts from,
            // which is what the developer did not write.
            if step == 0 || step > field.max - field.min {
                return Err(format!(
                    "{} field has a step {step} outside 1-{} in {item:?}",
                    field.name,
                    field.max - field.min
                ));
            }
            (spec, step)
        }
    };
    // A step with no explicit end runs to the field's maximum, so `*/15` and
    // `30/15` differ only in where they start.
    let (first, last) = match spec {
        "*" => (field.min, field.max),
        _ => match spec.split_once('-') {
            Some((from, to)) => (parse_value(from, field)?, parse_value(to, field)?),
            None => {
                let value = parse_value(spec, field)?;
                if step == 1 {
                    (value, value)
                } else {
                    (value, field.max)
                }
            }
        },
    };
    // A descending range is refused, and this is the one place celld declines
    // to follow Cloudflare. Cloudflare accepts it and wraps around the end of
    // the field for Quartz's sake, but takes one value too many at the low end:
    // `SAT-SUN` matches Friday there and `NOV-FEB` matches October, which
    // contradicts the worked example in saffron's own comment. Copying that
    // ships a schedule known to fire on a day nobody asked for, and correcting
    // it silently means the same expression means two things. Refusing stops
    // the deploy where the developer is watching instead.
    if first > last {
        return Err(format!(
            "{} field has a descending range in {item:?}; celld does not wrap a range around the end of a field",
            field.name
        ));
    }
    let mut bits = 0_u64;
    let mut value = first;
    while value <= last {
        bits |= 1 << value;
        value += step;
    }
    Ok(bits)
}

/// A run of ASCII digits and nothing else.
///
/// `str::parse` accepts a leading `+`, and Cloudflare's parser reads digits
/// only, so `0 0 * * +2` deploys here and fails there. Every number in an
/// expression goes through this.
fn parse_digits(text: &str) -> Option<u32> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn parse_value(text: &str, field: &Field) -> Result<u32, String> {
    let text = text.trim();
    let lower = text.to_ascii_lowercase();
    if let Some(index) = field.names.iter().position(|name| *name == lower) {
        return Ok(field.min + index as u32);
    }
    let value = parse_digits(text).ok_or_else(|| {
        if field.names.is_empty() {
            format!("{} field has a non-numeric value {text:?}", field.name)
        } else {
            format!(
                "{} field has a value {text:?} that is neither a number nor a name",
                field.name
            )
        }
    })?;
    if value < field.min || value > field.max {
        return Err(format!(
            "{} field has {value} outside {}-{}",
            field.name, field.min, field.max
        ));
    }
    Ok(value)
}

/// Does this expression match the minute containing `at_ms`?
pub fn matches(cron: &Cron, at_ms: Ms) -> bool {
    let minutes = at_ms.div_euclid(60_000);
    let day = minutes.div_euclid(1440);
    let time = minutes.rem_euclid(1440);
    day_matches(cron, day)
        && cron.hour & (1 << (time / 60)) != 0
        && cron.minute & (1 << (time % 60)) != 0
}

/// The day rule: with both day fields restricted a day matches when *either*
/// does, so `0 0 1 * MON` is the first of the month and every Monday. With one
/// field restricted, only that one decides. A field counts as restricted unless
/// it is literally `*`, so `*/2` is restricted and joins the union — the rule
/// Cloudflare applies, and not Vixie's, which reads the field's first character
/// and would call `*/2` unrestricted.
fn day_matches(cron: &Cron, day: i64) -> bool {
    let (year, month, day_of_month) = civil_from_days(day);
    if cron.month & (1 << month) == 0 {
        return false;
    }
    // 1970-01-01 was a Thursday, and Sunday is 0.
    let day_of_week = (day + 4).rem_euclid(7) as u32;
    let last = days_in_month(year, month);
    let by_dom = day_of_month_matches(&cron.day_of_month, day_of_month, day_of_week, last);
    let by_dow = day_of_week_matches(&cron.day_of_week, day_of_month, day_of_week, last);
    match (
        cron.day_of_month == DayOfMonth::All,
        cron.day_of_week == DayOfWeek::All,
    ) {
        (true, true) => true,
        (false, true) => by_dom,
        (true, false) => by_dow,
        (false, false) => by_dom || by_dow,
    }
}

/// Saturday and Sunday. `W` moves off these two and onto the nearest of the
/// other five, which is the whole content of the character.
fn is_weekend(day_of_week: u32) -> bool {
    day_of_week == 0 || day_of_week == 6
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
            if leap {
                29
            } else {
                28
            }
        }
    }
}

/// Each `W` arm below is Cloudflare's, condition for condition. `W` never
/// leaves the month: the day it names moves back to Friday or forward to
/// Monday, and the arms that look redundant are the ones that catch a month
/// which begins or ends on a weekend.
fn day_of_month_matches(field: &DayOfMonth, day: u32, weekday: u32, last: u32) -> bool {
    match *field {
        DayOfMonth::All => true,
        DayOfMonth::Days(days) => days & (1 << day) != 0,
        DayOfMonth::Last => day == last,
        DayOfMonth::LastOffset(offset) => day + offset == last,
        DayOfMonth::LastWeekday => {
            (!is_weekend(weekday) && day == last) || (weekday == 5 && last - day < 3)
        }
        DayOfMonth::LastOffsetWeekday(offset) => {
            let target = day + offset;
            (!is_weekend(weekday) && target == last)
                || (weekday == 1 && target > last && target - last < 3)
                || (weekday == 5 && target + 1 == last)
        }
        DayOfMonth::ClosestWeekday(target) => {
            (!is_weekend(weekday) && day == target)
                || (weekday == 1 && day > 1 && day - 1 == target)
                // The 1st falls on a Saturday and the 2nd on a Sunday, so the
                // nearest weekday inside the month is Monday the 3rd.
                || (weekday == 1 && day == 3 && target == 1)
                || (weekday == 5 && day + 1 == target)
                // The target is the last day of the month and falls on a
                // Sunday, so the move back is two days rather than one.
                || (weekday == 5 && day + 2 == target && target == last)
        }
    }
}

fn day_of_week_matches(field: &DayOfWeek, day: u32, weekday: u32, last: u32) -> bool {
    match *field {
        DayOfWeek::All => true,
        DayOfWeek::Days(days) => days & (1 << weekday) != 0,
        DayOfWeek::Last(wanted) => weekday == wanted && day + 7 > last,
        DayOfWeek::Nth(wanted, nth) => weekday == wanted && (day - 1) / 7 + 1 == nth,
    }
}

/// The first occurrence strictly after `after_ms`, or `None` when the
/// expression cannot match within [`MAX_LOOKAHEAD_DAYS`].
///
/// Strictly after is what makes a re-arm from inside the handler terminate: an
/// occurrence that returned itself would arm an alarm already due and spin.
pub fn next_after(cron: &Cron, after_ms: Ms) -> Option<Ms> {
    let start = after_ms.div_euclid(60_000) + 1;
    let mut time = start.rem_euclid(1440);
    for day in (start.div_euclid(1440)..).take(MAX_LOOKAHEAD_DAYS) {
        if day_matches(cron, day) {
            if let Some(found) = next_time_of_day(cron, time) {
                return Some((day * 1440 + found) * 60_000);
            }
        }
        time = 0;
    }
    None
}

/// The first minute-of-day at or after `from` whose hour and minute both
/// match, walking hours so an unmatched hour costs one step rather than sixty.
fn next_time_of_day(cron: &Cron, from: i64) -> Option<i64> {
    let (mut hour, mut minute) = (from / 60, from % 60);
    while hour < 24 {
        if cron.hour & (1 << hour) != 0 {
            while minute < 60 {
                if cron.minute & (1 << minute) != 0 {
                    return Some(hour * 60 + minute);
                }
                minute += 1;
            }
        }
        hour += 1;
        minute = 0;
    }
    None
}

/// The earliest next occurrence across every expression on a script. This is
/// what the reserved cron cell arms, because one alarm row holds one deadline
/// however many expressions the script declares.
pub fn next_across(crons: &[Cron], after_ms: Ms) -> Option<Ms> {
    crons
        .iter()
        .filter_map(|cron| next_after(cron, after_ms))
        .min()
}

/// Which expressions match a fired occurrence, by index. A single deadline can
/// belong to several expressions, and Cloudflare invokes `scheduled` once per
/// matching cron, so the fan-out happens here rather than at arm time.
pub fn matching(crons: &[Cron], at_ms: Ms) -> Vec<usize> {
    crons
        .iter()
        .enumerate()
        .filter(|(_, cron)| matches(cron, at_ms))
        .map(|(index, _)| index)
        .collect()
}

/// When to re-arm after a `scheduled` handler failed.
///
/// A cell has one alarm row, so a retry and the next occurrence compete for
/// the same deadline. The next occurrence wins whenever it is sooner: a
/// failing cron then retries with the usual backoff — `alarm::alarm_retry`
/// computes `retry_ms`, this does not fork it — but can never delay or skip a
/// scheduled run. A cron faster than the backoff simply never retries, which
/// is the right trade when the next attempt is seconds away anyway.
///
/// `retry_ms` is `None` when the retry ceiling is reached and `next_ms` is
/// `None` when no expression matches again; with both absent there is nothing
/// left to arm and the cell retires.
pub fn cron_rearm(next_ms: Option<Ms>, retry_ms: Option<Ms>) -> Option<Ms> {
    match (next_ms, retry_ms) {
        (Some(next), Some(retry)) => Some(next.min(retry)),
        (next, None) => next,
        (None, retry) => retry,
    }
}

/// The reserved cron cell's whole schedule decision, in one place so no
/// engine arm keeps a copy that can drift from it.
pub struct Plan {
    /// Which expressions the fired occurrence belongs to, by index.
    pub matching: Vec<usize>,
    /// When to arm next, or `None` when the schedule is exhausted and the
    /// cell retires.
    pub arm_at: Option<Ms>,
    /// Whether that deadline is the failure backoff rather than the next
    /// occurrence. `cron_rearm` takes the earlier and gives a tie to the
    /// occurrence, so `arm_at` alone cannot say, and the caller has to
    /// know: a retry owes the expressions that failed, an occurrence owes
    /// the ones that match it.
    pub arm_is_retry: bool,
}

/// `fired_ms` is the occurrence being handled, or `None` when the cell is
/// only arming. `retry` counts the failures within that occurrence and
/// `failed` says whether any expression of it is still owed.
pub fn plan(crons: &[Cron], fired_ms: Option<Ms>, now_ms: Ms, retry: i64, failed: bool) -> Plan {
    let matching = fired_ms.map(|at| matching(crons, at)).unwrap_or_default();
    let next = next_across(crons, now_ms);
    // The retry backoff is `alarm::alarm_retry`'s, not a second schedule:
    // a cron that fails behaves like any other failing alarm, except that
    // `cron_rearm` never lets the backoff outlast the next occurrence.
    let retry_at = failed
        .then(|| crate::alarm::alarm_retry(now_ms, retry, retry, true))
        .flatten();
    let arm_at = cron_rearm(next, retry_at);
    let arm_is_retry = match (arm_at, next) {
        (Some(at), Some(occurrence)) => at < occurrence,
        (Some(_), None) => true,
        (None, _) => false,
    };
    Plan {
        matching,
        arm_at,
        arm_is_retry,
    }
}
