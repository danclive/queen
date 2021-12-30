fn main() {
    #[cfg(not(target_os = "linux"))]
    compile_error!("This crate only support linux !");
}
