// ::START CLIPPY LINTS::
// -------------------------------------------------------------------
// Non-default Lints
// -------------------------------------------------------------------
// This block is auto-generated. Please do not edit it directly! If you would
// like to make changes, either adjust the global lint settings in the
// root-level Cargo.toml, override at your own package's Cargo.toml,
// or, where you need build-level config, update in `scripts/insert-clippy-lints.sh`.
// -------------------------------------------------------------------
// Always allow printing in test code
#![cfg_attr(test, allow(clippy::dbg_macro))]
#![cfg_attr(test, allow(clippy::print_stdout))]
#![cfg_attr(test, allow(clippy::print_stderr))]
// -------------------------------------------------------------------
// ::END CLIPPY LINTS::

pub mod stream;

pub use stream::SpecStreamExt;
