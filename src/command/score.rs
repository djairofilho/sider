//! Scores IEEE-754 sem NaN, com zero canônico e representação RESP2.

use std::cmp::Ordering;

#[derive(Clone, Copy, Debug)]
pub struct Score(f64);

impl Score {
    pub fn new(value: f64) -> Option<Self> {
        (!value.is_nan()).then_some(Self(if value == 0.0 { 0.0 } else { value }))
    }

    pub fn get(self) -> f64 {
        self.0
    }

    pub fn parse(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?;
        let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
        if unsigned.starts_with("0x") || unsigned.starts_with("0X") {
            return Self::new(parse_hex(text)?);
        }
        let number = text.parse::<f64>().ok()?;
        if number.is_infinite()
            && !unsigned.eq_ignore_ascii_case("inf")
            && !unsigned.eq_ignore_ascii_case("infinity")
        {
            return None;
        }
        if number == 0.0
            && unsigned
                .split(['e', 'E'])
                .next()?
                .bytes()
                .any(|byte| matches!(byte, b'1'..=b'9'))
        {
            return None;
        }
        Self::new(number)
    }

    pub fn to_bytes(self) -> bytes::Bytes {
        let value = self.0;
        if value == 0.0 {
            return bytes::Bytes::from_static(b"0");
        }
        if value.is_infinite() {
            return bytes::Bytes::from_static(if value < 0.0 { b"-inf" } else { b"inf" });
        }
        if value.abs() <= 4_611_686_018_427_387_904.0 && value.fract() == 0.0 {
            return bytes::Bytes::from((value as i64).to_string());
        }
        let (digits, power) = super::fpconv::digits(value.abs());
        let length = digits.len() as i32;
        let exponent = power + length - 1;
        let mut output = if value < 0.0 {
            String::from("-")
        } else {
            String::new()
        };
        if power >= 0 && exponent.abs() < length + 7 {
            output.push_str(&digits);
            output.extend(std::iter::repeat_n('0', power as usize));
        } else if power < 0 && (power > -7 || exponent.abs() < 4) {
            let point = length + power;
            if point <= 0 {
                output.push_str("0.");
                output.extend(std::iter::repeat_n('0', (-point) as usize));
                output.push_str(&digits);
            } else {
                output.push_str(&digits[..point as usize]);
                output.push('.');
                output.push_str(&digits[point as usize..]);
            }
        } else {
            output.push_str(&digits[..1]);
            if digits.len() > 1 {
                output.push('.');
                output.push_str(&digits[1..]);
            }
            output.push('e');
            if exponent >= 0 {
                output.push('+');
            }
            output.push_str(&exponent.to_string());
        }
        bytes::Bytes::from(output)
    }
}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

fn parse_hex(text: &str) -> Option<f64> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    let (mantissa, exponent) = unsigned[2..]
        .split_once(['p', 'P'])
        .unwrap_or((&unsigned[2..], "0"));
    let exponent_negative = exponent.starts_with('-');
    let exponent_digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
    if exponent_digits.is_empty() || !exponent_digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut power = exponent_digits.bytes().fold(0i64, |n, b| {
        n.saturating_mul(10).saturating_add(i64::from(b - b'0'))
    });
    if exponent_negative {
        power = -power;
    }
    let mut value = 0u64;
    let mut kept = 0;
    let mut discarded = 0i64;
    let mut fractional = 0i64;
    let mut point = false;
    let mut any = false;
    let mut sticky = false;
    for byte in mantissa.bytes() {
        if byte == b'.' {
            if point {
                return None;
            }
            point = true;
            continue;
        }
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        any = true;
        fractional += i64::from(point);
        if kept == 0 && digit == 0 {
            continue;
        }
        if kept < 16 {
            value = (value << 4) | u64::from(digit);
            kept += 1;
        } else {
            discarded += 1;
            sticky |= digit != 0;
        }
    }
    if !any {
        return None;
    }
    if value == 0 {
        return Some(0.0);
    }
    power = power
        .saturating_sub(fractional * 4)
        .saturating_add(discarded * 4);
    let highest = power.saturating_add(i64::from(63 - value.leading_zeros()));
    if highest > 1023 {
        return None;
    }
    let grid = highest.saturating_sub(52).max(-1074);
    let shift = grid.saturating_sub(power);
    let rounded = if shift <= 0 {
        value << (-shift) as u32
    } else if shift > 64 {
        0
    } else {
        let half = 1u64 << (shift - 1) as u32;
        let low = value & (half - 1);
        let truncated = if shift == 64 {
            0
        } else {
            value >> shift as u32
        };
        truncated + u64::from(value & half != 0 && (low != 0 || sticky || truncated & 1 != 0))
    };
    if rounded == 0 {
        return None;
    }
    let scale = if grid < -1022 {
        f64::from_bits(1u64 << (grid + 1074) as u32)
    } else {
        f64::from_bits(((grid + 1023) as u64) << 52)
    };
    let result = rounded as f64 * scale;
    result
        .is_finite()
        .then_some(if negative { -result } else { result })
}
