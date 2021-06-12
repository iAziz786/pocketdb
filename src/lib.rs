use serde::{Deserialize, Serialize};
use serde_json::{self};
use std::io::{self, prelude::*, BufReader, ErrorKind, SeekFrom};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    u64,
};

const END: &str = "\n";

pub struct Db {
    file: File,
    index_file: File,
    offset: HashMap<Vec<u8>, u64>,
    last_offset: u64,
}

#[derive(Serialize, Deserialize)]
pub struct KeyVal {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct KeyOffset {
    key: Vec<u8>,
    offset: u64,
}

impl Db {
    pub fn put(&mut self, key: Vec<u8>, val: Vec<u8>) {
        let kv = KeyVal { key, val };

        self.store_index(&kv.key, self.last_offset).unwrap();

        // store in the stable storage
        self.store_db(kv);
    }

    fn store_db(&mut self, kv: KeyVal) {
        let s = serde_json::to_string(&kv).unwrap();
        let s = s + END;
        self.file.seek(SeekFrom::Start(self.last_offset)).unwrap();
        self.file.write(s.as_bytes()).unwrap();
        self.last_offset += s.len() as u64;
    }

    fn fetch_db(&mut self, offset: u64) -> io::Result<KeyVal> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut reader = BufReader::new(&self.file);

        let mut buf = String::new();
        reader.read_line(&mut buf)?;

        let buf = buf.trim_end();
        let kv: KeyVal = serde_json::from_str(&buf)?;

        return Ok(kv);
    }

    pub fn get(&mut self, key: Vec<u8>) -> io::Result<KeyVal> {
        // find offset from the index
        if let Some(offset) = self.get_offset(&key) {
            // find from the db
            return Ok(self.fetch_db(offset)?);
        }

        Err(io::Error::new(ErrorKind::NotFound, "offset not found"))
    }
}

impl Db {
    /// Load the index from the stable storage
    fn boot_fill_index(&mut self) {
        // read each line of the index file and update that in the hashmap
        self.file.seek(SeekFrom::Start(0)).unwrap();
        let idx_reader = BufReader::new(&self.index_file);

        for line in idx_reader.lines() {
            if let Ok(text) = line {
                let text = text.trim_end_matches(END);
                let ko: KeyOffset = serde_json::from_str(text).unwrap();
                self.offset.insert(ko.key, ko.offset);
            }
        }
    }

    /// Find the offset of the data from the index
    fn get_offset(&mut self, key: &Vec<u8>) -> Option<u64> {
        if let Some(val) = self.offset.get(key) {
            Some(*val)
        } else {
            None
        }
    }

    /// Stores the index into a file. The same file is used to create the
    /// `offset` hashmap on calling `open()` which holds the key and it's offset.
    fn store_index(&mut self, key: &Vec<u8>, offset: u64) -> Result<(), io::Error> {
        let (key, offset) = self.store_index_stable(key, offset)?;
        // store index in the hashmap
        self.offset.insert(key, offset);

        return Ok(());
    }

    fn store_index_stable(
        &mut self,
        key: &Vec<u8>,
        offset: u64,
    ) -> Result<(Vec<u8>, u64), io::Error> {
        let kv = KeyOffset {
            key: (*key.to_owned()).to_vec(),
            offset,
        };
        let s = serde_json::to_string(&kv).unwrap();
        let s = s + "\n";
        // store the index to the stable store
        self.index_file.write(s.as_bytes())?;

        Ok((kv.key, kv.offset))
    }
}

pub fn open(path: &str) -> io::Result<Db> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;
    let idx_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path.to_owned() + ".idx")?;
    let mut db = Db {
        file,
        index_file: idx_file,
        offset: HashMap::new(),
        last_offset: 0,
    };

    db.boot_fill_index();

    Ok(db)
}

#[cfg(test)]
mod tests {
    use crate::open;

    #[test]
    fn write_content() {
        let mut db = open("mydb").unwrap();

        db.put(b"Hello".to_vec(), b"World".to_vec());
        db.put(b"Name".to_vec(), b"Aziz".to_vec());
        db.put(b"Age".to_vec(), b"25".to_vec());

        let kv = db.get(b"Hello".to_vec()).unwrap();
        assert_eq!(kv.val, b"World".to_vec());
        let kv = db.get(b"Name".to_vec()).unwrap();
        assert_eq!(kv.val, b"Aziz".to_vec());
        let kv = db.get(b"Age".to_vec()).unwrap();
        assert_eq!(String::from_utf8(kv.val).unwrap(), "25");
    }

    #[test]
    fn get_content() {
        let mut db = open("mydb").unwrap();

        let kv = db.get(b"Hello".to_vec()).unwrap();
        assert_eq!(kv.val, b"World".to_vec());
        let kv = db.get(b"Name".to_vec()).unwrap();
        assert_eq!(kv.val, b"Aziz".to_vec());
        let kv = db.get(b"Age".to_vec()).unwrap();
        assert_eq!(String::from_utf8(kv.val).unwrap(), "25");
    }
}
