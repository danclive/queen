fn main() {
    #[cfg(not(target_os = "linux"))]
    compile_error!("This crate does not support os other than linux !");
}
