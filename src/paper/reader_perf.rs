//! Per-thread Linux perf counters for the Figure 6 scan workers.

#[derive(Debug)]
pub(crate) struct ReaderPerfValue {
    pub(crate) event: &'static str,
    pub(crate) value: Option<u64>,
    pub(crate) raw_value: Option<u64>,
    pub(crate) time_enabled_ns: Option<u64>,
    pub(crate) time_running_ns: Option<u64>,
    pub(crate) error: Option<String>,
}

#[cfg(target_os = "linux")]
mod linux {
    use super::ReaderPerfValue;
    use std::os::fd::RawFd;

    const PERF_TYPE_HARDWARE: u32 = 0;
    const PERF_TYPE_SOFTWARE: u32 = 1;
    const PERF_TYPE_HW_CACHE: u32 = 3;
    const PERF_FORMAT_TOTAL_TIME_ENABLED: u64 = 1 << 0;
    const PERF_FORMAT_TOTAL_TIME_RUNNING: u64 = 1 << 1;
    const PERF_FLAG_FD_CLOEXEC: u64 = 1 << 3;
    const PERF_EVENT_IOC_ENABLE: libc::c_ulong = 0x2400;
    const PERF_EVENT_IOC_DISABLE: libc::c_ulong = 0x2401;
    const PERF_EVENT_IOC_RESET: libc::c_ulong = 0x2403;
    const PERF_ATTR_FLAG_DISABLED: u64 = 1 << 0;

    const HW_CPU_CYCLES: u64 = 0;
    const HW_INSTRUCTIONS: u64 = 1;
    const HW_CACHE_REFERENCES: u64 = 2;
    const HW_CACHE_MISSES: u64 = 3;
    const HW_BRANCH_INSTRUCTIONS: u64 = 4;
    const HW_BRANCH_MISSES: u64 = 5;

    const SW_PAGE_FAULTS: u64 = 2;
    const SW_PAGE_FAULTS_MIN: u64 = 5;
    const SW_PAGE_FAULTS_MAJ: u64 = 6;

    const CACHE_L1D: u64 = 0;
    const CACHE_LL: u64 = 2;
    const CACHE_DTLB: u64 = 3;
    const CACHE_OP_READ: u64 = 0;
    const CACHE_RESULT_ACCESS: u64 = 0;
    const CACHE_RESULT_MISS: u64 = 1;

    #[repr(C)]
    #[derive(Default)]
    struct PerfEventAttr {
        type_: u32,
        size: u32,
        config: u64,
        sample_period: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events: u32,
        bp_type: u32,
        config1: u64,
        config2: u64,
        branch_sample_type: u64,
        sample_regs_user: u64,
        sample_stack_user: u32,
        clockid: i32,
        sample_regs_intr: u64,
        aux_watermark: u32,
        sample_max_stack: u16,
        reserved_2: u16,
        aux_sample_size: u32,
        aux_action: u32,
        sig_data: u64,
    }

    #[repr(C)]
    #[derive(Default)]
    struct PerfRead {
        value: u64,
        time_enabled: u64,
        time_running: u64,
    }

    #[derive(Clone, Copy)]
    struct Event {
        name: &'static str,
        type_: u32,
        config: u64,
    }

    impl Event {
        const fn hardware(name: &'static str, config: u64) -> Self {
            Self {
                name,
                type_: PERF_TYPE_HARDWARE,
                config,
            }
        }

        const fn software(name: &'static str, config: u64) -> Self {
            Self {
                name,
                type_: PERF_TYPE_SOFTWARE,
                config,
            }
        }

        const fn cache(name: &'static str, cache: u64, result: u64) -> Self {
            Self {
                name,
                type_: PERF_TYPE_HW_CACHE,
                config: cache | (CACHE_OP_READ << 8) | (result << 16),
            }
        }
    }

    const CORE_EVENTS: &[Event] = &[
        Event::hardware("cycles", HW_CPU_CYCLES),
        Event::hardware("instructions", HW_INSTRUCTIONS),
        Event::hardware("branches", HW_BRANCH_INSTRUCTIONS),
        Event::hardware("branch-misses", HW_BRANCH_MISSES),
    ];
    const CACHE_EVENTS: &[Event] = &[
        Event::hardware("cache-references", HW_CACHE_REFERENCES),
        Event::hardware("cache-misses", HW_CACHE_MISSES),
        Event::cache("L1-dcache-loads", CACHE_L1D, CACHE_RESULT_ACCESS),
        Event::cache("L1-dcache-load-misses", CACHE_L1D, CACHE_RESULT_MISS),
    ];
    const MEMORY_EVENTS: &[Event] = &[
        Event::cache("LLC-loads", CACHE_LL, CACHE_RESULT_ACCESS),
        Event::cache("LLC-load-misses", CACHE_LL, CACHE_RESULT_MISS),
        Event::cache("dTLB-loads", CACHE_DTLB, CACHE_RESULT_ACCESS),
        Event::cache("dTLB-load-misses", CACHE_DTLB, CACHE_RESULT_MISS),
        Event::software("page-faults", SW_PAGE_FAULTS),
        Event::software("minor-faults", SW_PAGE_FAULTS_MIN),
        Event::software("major-faults", SW_PAGE_FAULTS_MAJ),
    ];

    enum CounterState {
        Open(RawFd),
        Unsupported(String),
    }

    struct Counter {
        event: Event,
        state: Option<CounterState>,
    }

    impl Counter {
        fn open(event: Event) -> Self {
            let attr = PerfEventAttr {
                type_: event.type_,
                size: std::mem::size_of::<PerfEventAttr>() as u32,
                config: event.config,
                read_format: PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING,
                flags: PERF_ATTR_FLAG_DISABLED,
                ..Default::default()
            };
            let fd = unsafe {
                libc::syscall(
                    libc::SYS_perf_event_open,
                    &attr as *const PerfEventAttr,
                    0_i32,
                    -1_i32,
                    -1_i32,
                    PERF_FLAG_FD_CLOEXEC,
                ) as RawFd
            };
            let state = if fd < 0 {
                CounterState::Unsupported(std::io::Error::last_os_error().to_string())
            } else {
                CounterState::Open(fd)
            };
            Self {
                event,
                state: Some(state),
            }
        }

        fn enable(&mut self) {
            let Some(CounterState::Open(fd)) = self.state.as_ref() else {
                return;
            };
            let fd = *fd;
            let reset = unsafe { libc::ioctl(fd, PERF_EVENT_IOC_RESET, 0) };
            let enable = if reset == 0 {
                unsafe { libc::ioctl(fd, PERF_EVENT_IOC_ENABLE, 0) }
            } else {
                -1
            };
            if reset != 0 || enable != 0 {
                let error = std::io::Error::last_os_error().to_string();
                unsafe { libc::close(fd) };
                self.state = Some(CounterState::Unsupported(error));
            }
        }

        fn disable(&self) {
            if let Some(CounterState::Open(fd)) = self.state.as_ref() {
                unsafe {
                    libc::ioctl(*fd, PERF_EVENT_IOC_DISABLE, 0);
                }
            }
        }

        fn finish(mut self) -> ReaderPerfValue {
            match self.state.take().expect("counter state") {
                CounterState::Unsupported(error) => ReaderPerfValue {
                    event: self.event.name,
                    value: None,
                    raw_value: None,
                    time_enabled_ns: None,
                    time_running_ns: None,
                    error: Some(error),
                },
                CounterState::Open(fd) => {
                    let mut reading = PerfRead::default();
                    let bytes = unsafe {
                        libc::read(
                            fd,
                            &mut reading as *mut PerfRead as *mut libc::c_void,
                            std::mem::size_of::<PerfRead>(),
                        )
                    };
                    unsafe { libc::close(fd) };
                    if bytes != std::mem::size_of::<PerfRead>() as isize {
                        return ReaderPerfValue {
                            event: self.event.name,
                            value: None,
                            raw_value: None,
                            time_enabled_ns: None,
                            time_running_ns: None,
                            error: Some(std::io::Error::last_os_error().to_string()),
                        };
                    }
                    let scaled = if reading.time_running == 0 {
                        None
                    } else {
                        Some(
                            ((reading.value as f64) * (reading.time_enabled as f64)
                                / (reading.time_running as f64))
                                .round() as u64,
                        )
                    };
                    ReaderPerfValue {
                        event: self.event.name,
                        value: scaled,
                        raw_value: Some(reading.value),
                        time_enabled_ns: Some(reading.time_enabled),
                        time_running_ns: Some(reading.time_running),
                        error: scaled.is_none().then(|| "counter never scheduled".into()),
                    }
                }
            }
        }
    }

    impl Drop for Counter {
        fn drop(&mut self) {
            if let Some(CounterState::Open(fd)) = self.state.as_ref() {
                unsafe { libc::close(*fd) };
            }
        }
    }

    pub(crate) struct ReaderPerf {
        counters: Vec<Counter>,
    }

    impl ReaderPerf {
        pub(crate) fn prepare() -> Self {
            let events = match std::env::var("CMVBT_READER_PERF_GROUP").ok().as_deref() {
                Some("core") => CORE_EVENTS,
                Some("cache") => CACHE_EVENTS,
                Some("memory") => MEMORY_EVENTS,
                _ => &[],
            };
            Self {
                counters: events.iter().copied().map(Counter::open).collect(),
            }
        }

        pub(crate) fn start(&mut self) {
            self.counters.iter_mut().for_each(Counter::enable);
        }

        pub(crate) fn finish(self) -> Vec<ReaderPerfValue> {
            self.counters.iter().for_each(Counter::disable);
            self.counters.into_iter().map(Counter::finish).collect()
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::ReaderPerf;

#[cfg(not(target_os = "linux"))]
pub(crate) struct ReaderPerf;

#[cfg(not(target_os = "linux"))]
impl ReaderPerf {
    pub(crate) fn prepare() -> Self {
        Self
    }

    pub(crate) fn start(&mut self) {}

    pub(crate) fn finish(self) -> Vec<ReaderPerfValue> {
        vec![]
    }
}
