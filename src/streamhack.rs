//! Hack a stream of JSON-with-trailing-commas into "proper" JSON.
//!
//! I misconfigured my Fastly logging config to have trailing commas.
//! Well and good, except that serde_json ~rightly rejects that.
//!
//! So: hack a stream, replace ",\w*}\w*" with "no comma".

use std::sync::OnceLock;

use regex_lite::Regex;

pub struct CommaHacker<I> {
    input: I,
    buffer: Vec<u8>,
}

static TAILMATCH: OnceLock<Regex> = OnceLock::new();
impl<I> CommaHacker<I> {
    pub fn new(input: I) -> Self {
        CommaHacker {
            input,
            buffer: Vec::new(),
        }
    }
}

impl<I> CommaHacker<I>
where
    I: std::io::Read,
{
    fn read_from_buffer(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = std::cmp::min(self.buffer.len(), buf.len());
        buf[0..len].copy_from_slice(&self.buffer[0..len]);
        self.buffer = Vec::from(&self.buffer[len..]);
        Ok(len)
    }
}

impl<I> std::io::Read for CommaHacker<I>
where
    I: std::io::BufRead,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.buffer.is_empty() {
            return self.read_from_buffer(buf);
        }
        let mut s = String::new();
        self.input.read_line(&mut s)?;

        let re = TAILMATCH.get_or_init(|| Regex::new(r#",\s*}\s*$"#).unwrap());
        self.buffer = re.replace(&s, "}").as_bytes().into();
        self.read_from_buffer(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::streamhack::CommaHacker;

    #[test]
    fn strips_comma_and_parses() {
        use std::io::{BufReader, Cursor};

        const OBJ: &str = r#" { "hello": "world", "are you": 1, }"#;
        let cursor = CommaHacker::new(BufReader::new(Cursor::new(OBJ)));
        let v: serde_json::Value = serde_json::from_reader(cursor).unwrap();
        assert_eq!(
            v.get("hello"),
            Some(serde_json::Value::String("world".to_string())).as_ref()
        );
        assert_eq!(v.get("are you"), Some(serde_json::json!(1)).as_ref());
    }
}
