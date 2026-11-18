// Adds the Vulkan SDK `lib/` directory to the linker search path so `-lvulkan`
// resolves at link time. The path is derived from `CMAKE_ARGS` (the same env
// var `transcribe-cpp-sys` forwards to CMake), so a single `CMAKE_ARGS` value
// covers both CMake's SDK search and the linker.
//
// By default we emit nothing and rely on the standard system search paths.
fn main() {
    println!("cargo:rerun-if-env-changed=CMAKE_ARGS");

    if let Ok(args) = std::env::var("CMAKE_ARGS") {
        if let Some(prefix) = extract_prefix(&args) {
            let lib = std::path::Path::new(&prefix).join("lib");
            println!("cargo:rustc-link-search=native={}", lib.display());
        }
    }
}

/// Pulls the path out of `-DCMAKE_PREFIX_PATH=<path>` from a `CMAKE_ARGS` value.
fn extract_prefix(args: &str) -> Option<&str> {
    for token in args.split_whitespace() {
        if let Some(value) = token.strip_prefix("-DCMAKE_PREFIX_PATH=") {
            return Some(value);
        }
    }
    None
}
