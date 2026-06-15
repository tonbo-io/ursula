use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct HumanDuration(Duration);

impl HumanDuration {
    pub const fn as_duration(&self) -> Duration {
        self.0
    }
    pub const fn milli(n: u64) -> Self {
        Self(Duration::from_millis(n))
    }
    pub const fn sec(n: u64) -> Self {
        Self(Duration::from_secs(n))
    }
    pub const fn min(n: u64) -> Self {
        Self(Duration::from_secs(n * 60))
    }
    pub const fn hour(n: u64) -> Self {
        Self(Duration::from_secs(n * 3600))
    }
    pub const fn day(n: u64) -> Self {
        Self(Duration::from_secs(n * 86400))
    }
}

impl From<Duration> for HumanDuration {
    fn from(d: Duration) -> Self {
        Self(d)
    }
}

impl fmt::Display for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.0.as_secs();
        let millis = self.0.subsec_millis();
        if secs == 0 && millis > 0 {
            write!(f, "{}ms", millis)
        } else if secs.is_multiple_of(86400) && secs > 0 && millis == 0 {
            write!(f, "{}d", secs / 86400)
        } else if secs.is_multiple_of(3600) && secs > 0 && millis == 0 {
            write!(f, "{}h", secs / 3600)
        } else if secs.is_multiple_of(60) && secs > 0 && millis == 0 {
            write!(f, "{}m", secs / 60)
        } else if millis > 0 {
            write!(f, "{}.{:03}s", secs, millis)
        } else {
            write!(f, "{}s", secs)
        }
    }
}

impl FromStr for HumanDuration {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty duration string".into());
        }
        if let Ok(ms) = s.parse::<u64>() {
            return Ok(Self(Duration::from_millis(ms)));
        }
        let unit_start = s
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .ok_or_else(|| format!("duration '{s}' missing unit suffix"))?;
        let (num_part, unit) = s.split_at(unit_start);
        let value: f64 = num_part
            .parse()
            .map_err(|_| format!("invalid duration number '{num_part}'"))?;
        let duration = match unit {
            "ms" => {
                if value.fract() != 0.0 {
                    return Err(format!("duration '{s}': milliseconds must be an integer"));
                }
                Duration::from_millis(value as u64)
            }
            "s" => Duration::try_from_secs_f64(value)
                .map_err(|_| format!("duration '{s}' out of range"))?,
            "m" => Duration::try_from_secs_f64(value * 60.0)
                .map_err(|_| format!("duration '{s}' out of range"))?,
            "h" => Duration::try_from_secs_f64(value * 3600.0)
                .map_err(|_| format!("duration '{s}' out of range"))?,
            "d" => Duration::try_from_secs_f64(value * 86400.0)
                .map_err(|_| format!("duration '{s}' out of range"))?,
            other => return Err(format!("unknown duration unit '{other}'")),
        };
        Ok(Self(duration))
    }
}

impl Serialize for HumanDuration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = HumanDuration;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    f,
                    "a duration string like '1s', '5m', '1h' or an integer (milliseconds)"
                )
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                if v < 0 {
                    return Err(E::custom("duration cannot be negative"));
                }
                Ok(HumanDuration(Duration::from_millis(v as u64)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(HumanDuration(Duration::from_millis(v)))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse().map_err(E::custom)
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct HumanSize(u64);

impl HumanSize {
    pub const fn as_bytes(&self) -> u64 {
        self.0
    }
    pub const fn bytes(n: u64) -> Self {
        Self(n)
    }
    pub const fn kib(n: u64) -> Self {
        Self(n * 1024)
    }
    pub const fn mib(n: u64) -> Self {
        Self(n * 1024 * 1024)
    }
    pub const fn gib(n: u64) -> Self {
        Self(n * 1024 * 1024 * 1024)
    }
}

impl From<u64> for HumanSize {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl fmt::Display for HumanSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0;
        if bytes >= 1024 * 1024 * 1024 && bytes.is_multiple_of(1024 * 1024 * 1024) {
            write!(f, "{}GiB", bytes / (1024 * 1024 * 1024))
        } else if bytes >= 1024 * 1024 && bytes.is_multiple_of(1024 * 1024) {
            write!(f, "{}MiB", bytes / (1024 * 1024))
        } else if bytes >= 1024 && bytes.is_multiple_of(1024) {
            write!(f, "{}KiB", bytes / 1024)
        } else {
            write!(f, "{}B", bytes)
        }
    }
}

impl FromStr for HumanSize {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty size string".into());
        }
        if let Ok(bytes) = s.parse::<u64>() {
            return Ok(Self(bytes));
        }
        let unit_start = s
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .ok_or_else(|| format!("size '{s}' missing unit suffix"))?;
        let (num_part, unit) = s.split_at(unit_start);
        let value: f64 = num_part
            .parse()
            .map_err(|_| format!("invalid size number '{num_part}'"))?;
        let multiplier: f64 = match unit {
            "B" | "b" => 1.0,
            "K" | "KB" | "k" | "kb" => 1000.0,
            "Ki" | "KiB" | "kib" => 1024.0,
            "M" | "MB" | "m" | "mb" => 1000.0 * 1000.0,
            "Mi" | "MiB" | "mib" => 1024.0 * 1024.0,
            "G" | "GB" | "g" | "gb" => 1000.0 * 1000.0 * 1000.0,
            "Gi" | "GiB" | "gib" => 1024.0 * 1024.0 * 1024.0,
            other => return Err(format!("unknown size unit '{other}'")),
        };
        let result = value * multiplier;
        if !result.is_finite() || result > u64::MAX as f64 {
            return Err(format!("size '{s}' overflows u64"));
        }
        Ok(Self(result as u64))
    }
}

impl Serialize for HumanSize {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HumanSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = HumanSize;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    f,
                    "a size string like '256MiB', '1.5GiB' or an integer (bytes)"
                )
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                if v < 0 {
                    return Err(E::custom("size cannot be negative"));
                }
                Ok(HumanSize(v as u64))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(HumanSize(v))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse().map_err(E::custom)
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}
