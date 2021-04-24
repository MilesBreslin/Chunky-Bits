use std::fmt;

use tokio::{
    select,
    sync::{
        mpsc,
        oneshot,
    },
    time::{
        Duration,
        Instant,
    },
};

use crate::{
    error::LocationError,
    file::Location,
};

pub struct ProfileReporter {
    report: oneshot::Receiver<ProfileReport>,
    quit: oneshot::Sender<()>,
}

impl ProfileReporter {
    pub async fn profile(self) -> ProfileReport {
        let ProfileReporter { report, quit } = self;
        drop(quit);
        report.await.unwrap()
    }
}

pub fn new_profiler() -> (Profiler, ProfileReporter) {
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<ResultLog>();
    let (profile_tx, profile_rx) = oneshot::channel::<ProfileReport>();
    let (drop_tx, mut drop_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let mut profile = ProfileReport(vec![]);
        loop {
            select! {
                _ = &mut drop_rx => {
                    let _ = profile_tx.send(profile);
                    return;
                },
                result = log_rx.recv() => {
                    match result {
                        Some(result) => {
                            profile.0.push(result);
                        },
                        None => {
                            let _ = profile_tx.send(profile);
                            return;
                        },
                    }
                },
            }
        }
    });
    let reporter = ProfileReporter {
        report: profile_rx,
        quit: drop_tx,
    };
    let profiler = Profiler(log_tx);
    (profiler, reporter)
}

enum ResultLog {
    Read(ReadResult),
    Write(WriteResult),
}

impl From<ReadResult> for ResultLog {
    fn from(res: ReadResult) -> ResultLog {
        ResultLog::Read(res)
    }
}

impl From<WriteResult> for ResultLog {
    fn from(res: WriteResult) -> ResultLog {
        ResultLog::Write(res)
    }
}

struct ReadResult {
    result: Result<usize, String>,
    location: Location,
    start_time: Instant,
    end_time: Instant,
}

struct WriteResult {
    result: Result<(), String>,
    location: Location,
    length: usize,
    start_time: Instant,
    end_time: Instant,
}

trait GeneralResult {
    fn error(&self) -> Result<(), &str>;
    fn length(&self) -> Option<usize>;
    fn location(&self) -> &Location;
    fn start_time(&self) -> &Instant;
    fn end_time(&self) -> &Instant;
    fn duration(&self) -> Duration {
        self.end_time().duration_since(*self.start_time())
    }
}

impl GeneralResult for ReadResult {
    fn error(&self) -> Result<(), &str> {
        match &self.result {
            Ok(_) => Ok(()),
            Err(err) => Err(&err),
        }
    }

    fn length(&self) -> Option<usize> {
        self.result.as_ref().ok().copied()
    }

    fn location(&self) -> &Location {
        &self.location
    }

    fn start_time(&self) -> &Instant {
        &self.start_time
    }

    fn end_time(&self) -> &Instant {
        &self.end_time
    }
}

impl GeneralResult for WriteResult {
    fn error(&self) -> Result<(), &str> {
        match &self.result {
            Ok(_) => Ok(()),
            Err(err) => Err(&err),
        }
    }

    fn length(&self) -> Option<usize> {
        Some(self.length)
    }

    fn location(&self) -> &Location {
        &self.location
    }

    fn start_time(&self) -> &Instant {
        &self.start_time
    }

    fn end_time(&self) -> &Instant {
        &self.end_time
    }
}

macro_rules! result_log_copy_children {
    ($self:ident, $func:ident) => {
        match $self {
            ResultLog::Read(res) => <ReadResult as GeneralResult>::$func(res),
            ResultLog::Write(res) => <WriteResult as GeneralResult>::$func(res),
        }
    };
}

impl GeneralResult for ResultLog {
    fn error(&self) -> Result<(), &str> {
        result_log_copy_children!(self, error)
    }

    fn length(&self) -> Option<usize> {
        result_log_copy_children!(self, length)
    }

    fn location(&self) -> &Location {
        result_log_copy_children!(self, location)
    }

    fn start_time(&self) -> &Instant {
        result_log_copy_children!(self, start_time)
    }

    fn end_time(&self) -> &Instant {
        result_log_copy_children!(self, end_time)
    }
}

#[derive(Clone)]
pub struct Profiler(mpsc::UnboundedSender<ResultLog>);

impl Profiler {
    pub(super) fn log_read(
        &self,
        result: &Result<Vec<u8>, LocationError>,
        location: Location,
        start_time: Instant,
    ) {
        let result = match result {
            Ok(bytes) => Ok(bytes.len()),
            Err(err) => Err(format!("{}", err)),
        };
        let _ = self.0.send(
            ReadResult {
                result,
                location,
                start_time,
                end_time: Instant::now(),
            }
            .into(),
        );
    }

    pub(super) fn log_write(
        &self,
        result: &Result<(), LocationError>,
        location: Location,
        length: usize,
        start_time: Instant,
    ) {
        let result = match result {
            Ok(_) => Ok(()),
            Err(err) => Err(format!("{}", err)),
        };
        let _ = self.0.send(
            WriteResult {
                result,
                location,
                length,
                start_time,
                end_time: Instant::now(),
            }
            .into(),
        );
    }
}

pub struct ProfileReport(Vec<ResultLog>);

impl fmt::Display for ProfileReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ReadAvg<{:?}ms> WriteAvg<{:?}ms> Total<{:?}ms> Total<{}B>",
            self.average_read_duration().map(|d| d.as_millis()),
            self.average_write_duration().map(|d| d.as_millis()),
            self.total_time().map(|d| d.as_millis()),
            self.total_bytes(),
        )
    }
}

impl ProfileReport {
    pub fn average_read_duration(&self) -> Option<Duration> {
        let writes = self.reads();
        let durations = Self::duration(writes);
        Self::average_duration(durations)
    }

    pub fn average_write_duration(&self) -> Option<Duration> {
        let writes = self.writes();
        let durations = Self::duration(writes);
        Self::average_duration(durations)
    }

    pub fn start_time(&self) -> Option<Instant> {
        self.0.first().map(|res| *res.start_time())
    }

    pub fn end_time(&self) -> Option<Instant> {
        self.0.last().map(|res| *res.end_time())
    }

    pub fn total_time(&self) -> Option<Duration> {
        if let (Some(start), Some(end)) = (self.start_time(), self.end_time()) {
            Some(end.duration_since(start))
        } else {
            None
        }
    }

    pub fn total_bytes(&self) -> usize {
        Self::success(self.0.iter())
            .map(|res| res.length().unwrap())
            .sum()
    }

    fn average_duration(mut durations: impl Iterator<Item = Duration>) -> Option<Duration> {
        if let Some(mut duration) = durations.next() {
            let mut count: u32 = 1;
            for d in durations {
                duration += d;
                count += 1;
            }
            Some(duration / count)
        } else {
            None
        }
    }

    fn duration<'a>(
        iter: impl Iterator<Item = &'a (impl GeneralResult + 'static)> + 'a,
    ) -> impl Iterator<Item = Duration> + 'a {
        iter.map(move |res| res.duration())
    }

    fn success<'a, T>(iter: impl Iterator<Item = &'a T> + 'a) -> impl Iterator<Item = &'a T> + 'a
    where
        T: GeneralResult + 'static,
    {
        iter.filter(|res| res.error().is_ok())
    }

    fn writes(&self) -> impl Iterator<Item = &WriteResult> {
        self.0.iter().filter_map(|res| match res {
            ResultLog::Write(res) => Some(res),
            _ => None,
        })
    }

    fn reads(&self) -> impl Iterator<Item = &ReadResult> {
        self.0.iter().filter_map(|res| match res {
            ResultLog::Read(res) => Some(res),
            _ => None,
        })
    }
}
