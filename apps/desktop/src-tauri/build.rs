fn main() {
    // Tauri desktop links the app lib statically (main.rs -> saiwork2_lib::run),
    // so the cdylib needs no exported symbols. Without this flag rustc passes
    // --export-all-symbols for cdylibs and the entire dependency graph (~102k
    // symbols) lands in the export table, overflowing the PE 16-bit ordinal limit
    // (GNU ld: "export ordinal too large"; lld: "too many exported symbols").
    // Scoped to the cdylib only — rlib/staticlib/bin links are unaffected.
    println!("cargo::rustc-cdylib-link-arg=--exclude-all-symbols");
    tauri_build::build()
}
