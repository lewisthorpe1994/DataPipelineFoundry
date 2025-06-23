use std::path::Path;

pub trait IsFileExtension {
    fn is_extension(&self, ext: &str) -> bool;
}
impl IsFileExtension for Path {
    fn is_extension(&self, ext: &str) -> bool {
        self.extension().and_then(|e| e.to_str()) == Some(ext)
    }
}