//! `go!` and `select!` macros — public spawn / multiplex syntax.
//!
//! TODO(steps 4, 14, 18): expand `go!(|| { ... })` to a runtime spawn call;
//! expand `select!` to a `selectgo` invocation with Go-style arms
//! (`v = <-rx`, `tx <- val`, `default`).
