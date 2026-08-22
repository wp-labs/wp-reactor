//! Window actor coverage: shutdown flush, sender-drop pending flush (with the
//! sequence-gap warning), budget acquisition on a closed semaphore, and the
//! per-append outcome reporter (appended vs late-dropped).

mod coverage_extra;
mod coverage_more;
