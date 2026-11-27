use crate::queue::Queue;
use serde::{Deserialize, Serialize};
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

const VERSION: u32 = 1;

#[cfg(all(test, unix))]
#[path = "../tests/unit/snapshot.rs"]
pub(crate) mod tests;

#[derive(Deserialize)]
struct Snapshot {
    version: u32,
    queue: Queue,
}

#[derive(Serialize)]
struct SnapshotRef<'a> {
    version: u32,
    queue: &'a Queue,
}

pub struct Store {
    path: PathBuf,
    temp: PathBuf,
    dir: File,
    _lock: File,
}

pub struct SaveError {
    pub source: io::Error,
    pub replaced: bool,
}

impl Store {
    pub fn open(path: PathBuf) -> io::Result<(Self, Queue)> {
        let name = path.file_name().ok_or_else(|| io::Error::new(
            io::ErrorKind::InvalidInput, "state file requires a filename",
        ))?;
        let parent = path.parent().filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let parent = parent.canonicalize()?;
        let path = parent.join(name);
        let dir = File::open(parent)?;
        let lock = private_options().create(true).truncate(false).open(sibling(&path, ".lock"))?;
        lock_exclusive(&lock)?;
        let temp = sibling(&path, ".tmp");
        match fs::remove_file(&temp) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let store = Self { path, temp, dir, _lock: lock };
        let queue = store.load()?;
        Ok((store, queue))
    }

    fn load(&self) -> io::Result<Queue> {
        match fs::symlink_metadata(&self.path) {
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "state path must be a regular file"));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Queue::default()),
            Err(error) => return Err(error),
            _ => {}
        }
        let file = File::open(&self.path)?;
        let snapshot: Snapshot = serde_json::from_reader(BufReader::new(file))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if snapshot.version != VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!(
                "unsupported queue snapshot version {}", snapshot.version
            )));
        }
        snapshot.queue.validate_snapshot()?;
        Ok(snapshot.queue)
    }

    pub fn save(&self, queue: &Queue) -> Result<(), SaveError> {
        let file = private_options().create_new(true).open(&self.temp)
            .map_err(|source| SaveError { source, replaced: false })?;
        let result = (|| -> io::Result<()> {
            let mut output = BufWriter::new(file);
            serde_json::to_writer(&mut output, &SnapshotRef { version: VERSION, queue })?;
            output.flush()?;
            output.get_ref().sync_all()?;
            drop(output);
            fs::rename(&self.temp, &self.path)
        })();
        if let Err(source) = result {
            let _ = fs::remove_file(&self.temp);
            return Err(SaveError { source, replaced: false });
        }
        self.dir.sync_all().map_err(|source| SaveError { source, replaced: true })
    }
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = OsString::from(path.file_name().expect("state filename"));
    name.push(suffix);
    path.with_file_name(name)
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

#[cfg(unix)]
fn lock_exclusive(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    loop {
        // The Store keeps this descriptor open for its entire lifetime.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => return Err(io::Error::new(
                io::ErrorKind::WouldBlock, "queue state file already has a broker",
            )),
            _ => return Err(error),
        }
    }
}

#[cfg(not(unix))]
fn lock_exclusive(_file: &File) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "queue persistence requires Unix"))
}
