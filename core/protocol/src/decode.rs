use crate::*;
struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let end = self.pos.checked_add(n).ok_or("length")?;
        let v = self.b.get(self.pos..end).ok_or("truncated")?;
        self.pos = end;
        Ok(v)
    }
    fn u8(&mut self) -> Result<u8, &'static str> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, &'static str> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, &'static str> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn count(&mut self, max: usize) -> Result<usize, &'static str> {
        let n = self.u32()? as usize;
        if n > max { Err("length limit") } else { Ok(n) }
    }
    fn bytes(&mut self, max: usize) -> Result<&'a [u8], &'static str> {
        let n = self.count(max)?;
        self.take(n)
    }
    fn text(&mut self, max: usize) -> Result<String, &'static str> {
        std::str::from_utf8(self.bytes(max)?)
            .map(str::to_owned)
            .map_err(|_| "UTF-8")
    }
    fn value(&mut self) -> Result<Value, &'static str> {
        let v = match self.u8()? {
            0 => Value::Null,
            1 => Value::Integer(self.u64()? as i64),
            2 => Value::Real(f64::from_bits(self.u64()?)),
            3 => Value::Text(self.text(MAX_CELL)?),
            4 => Value::Blob(self.bytes(MAX_CELL)?.to_vec()),
            _ => return Err("value tag"),
        };
        v.validate()?;
        Ok(v)
    }
}
impl OpId {
    pub fn decode(b: &[u8]) -> Result<Self, &'static str> {
        let mut r = Reader { b, pos: 0 };
        let id = Self {
            dataset: r.text(128)?,
            device: r.text(128)?,
            incarnation: r.text(128)?,
            seq: r.u64()?,
            index: r.u32()?,
            schema: r.u32()?,
        };
        Identity {
            dataset: id.dataset.clone(),
            device: id.device.clone(),
            incarnation: id.incarnation.clone(),
        }
        .validate()?;
        if r.pos != b.len()
            || id.seq == 0
            || id.seq >= i64::MAX as u64
            || id.index >= MAX_OPS as u32
            || id.schema == 0
        {
            return Err("op identity");
        };
        Ok(id)
    }
    /// Identity excludes schema: a different schema under the same operation identity is a fork.
    pub fn identity_key(&self) -> (String, String, String, u64, u32) {
        (
            self.dataset.clone(),
            self.device.clone(),
            self.incarnation.clone(),
            self.seq,
            self.index,
        )
    }
}
impl Packet {
    pub fn decode(b: &[u8]) -> Result<Self, &'static str> {
        if b.len() > MAX_PACKET {
            return Err("packet size");
        }
        let mut r = Reader { b, pos: 0 };
        if r.take(8)? != b"LFS1\0\0\0\x01" {
            return Err("protocol version");
        }
        let identity = Identity {
            dataset: r.text(128)?,
            device: r.text(128)?,
            incarnation: r.text(128)?,
        };
        let schema = r.u32()?;
        let seq = r.u64()?;
        let predecessor = match r.u8()? {
            0 => None,
            1 => Some(r.take(32)?.try_into().unwrap()),
            _ => return Err("predecessor tag"),
        };
        let n = r.count(MAX_OPS)?;
        let mut operations = Vec::new();
        for index in 0..n {
            let table = r.u32()?;
            let Value::Text(key) = r.value()? else {
                return Err("key type");
            };
            let pn = r.count(256)?;
            let mut parents = Vec::new();
            for _ in 0..pn {
                parents.push(r.bytes(512)?.to_vec());
            }
            let row = match r.u8()? {
                0 => None,
                1 => {
                    let cols = r.count(64)?;
                    let mut v = Vec::new();
                    for _ in 0..cols {
                        v.push(r.value()?)
                    }
                    Some(v)
                }
                _ => return Err("row tag"),
            };
            operations.push(Operation {
                id: OpId {
                    dataset: identity.dataset.clone(),
                    device: identity.device.clone(),
                    incarnation: identity.incarnation.clone(),
                    seq,
                    index: index as u32,
                    schema,
                },
                table,
                key,
                parents,
                row,
            });
        }
        let p = Self {
            identity,
            schema,
            seq,
            predecessor,
            operations,
        };
        if r.pos != b.len() || p.encode()? != b {
            return Err("noncanonical packet");
        };
        Ok(p)
    }
}
