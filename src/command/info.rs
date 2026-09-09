/// Selection of fixed-cardinality sections; unknown names are not retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InfoSections(u8);

impl InfoSections {
    pub const SERVER: u8 = 1;
    pub const CLIENTS: u8 = 2;
    pub const STATS: u8 = 4;
    pub const MEMORY: u8 = 8;
    pub const PERSISTENCE: u8 = 16;
    pub const CONFIG: u8 = 32;
    pub const REPLICATION: u8 = 64;
    pub const ALL: Self = Self(127);

    pub fn from_names<'a>(names: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut selection = 0;
        let mut any = false;
        for name in names {
            any = true;
            let bit = if name.eq_ignore_ascii_case(b"server") {
                Self::SERVER
            } else if name.eq_ignore_ascii_case(b"clients") {
                Self::CLIENTS
            } else if name.eq_ignore_ascii_case(b"stats") {
                Self::STATS
            } else if name.eq_ignore_ascii_case(b"memory") {
                Self::MEMORY
            } else if name.eq_ignore_ascii_case(b"persistence") {
                Self::PERSISTENCE
            } else if name.eq_ignore_ascii_case(b"config") {
                Self::CONFIG
            } else if name.eq_ignore_ascii_case(b"replication") {
                Self::REPLICATION
            } else if name.eq_ignore_ascii_case(b"all")
                || name.eq_ignore_ascii_case(b"default")
                || name.eq_ignore_ascii_case(b"everything")
            {
                Self::ALL.0
            } else {
                0
            };
            selection |= bit;
        }
        if any { Self(selection) } else { Self::ALL }
    }

    pub fn contains(self, section: u8) -> bool {
        self.0 & section != 0
    }
}
