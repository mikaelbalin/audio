// Adds the Homebrew (Linuxbrew) library directory to the linker search path so
// `-lvulkan` (emitted by the transcribe-cpp ggml Vulkan backend) resolves at
// link time. On this machine the Vulkan loader lives under Homebrew, which is
// not on the linker's default search path.
fn main() {
    // Re-run if the value is changed via env.
    println!("cargo:rerun-if-env-changed=HOMEBREW_PREFIX");

    let prefix =
        std::env::var("HOMEBREW_PREFIX").unwrap_or_else(|_| "/home/linuxbrew/.linuxbrew".into());
    let lib = std::path::Path::new(&prefix).join("lib");
    println!("cargo:rustc-link-search=native={}", lib.display());
}
