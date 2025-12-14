fn main() -> pyo3_stub_gen::Result<()> {
    dpf_python::stub_info()?.generate()?;
    Ok(())
}
