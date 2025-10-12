use crate::traits::IsFileExtension;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

pub fn read_sql_file(
    models_dir: &str,
    node_path: &PathBuf,
    compile_path: &str,
) -> Result<String, std::io::Error> {
    let rel_path = Path::new(&node_path)
        .strip_prefix(models_dir)
        .unwrap_or(Path::new(&node_path));
    let sql_path = Path::new(compile_path).join(rel_path);
    let sql = std::fs::read_to_string(&sql_path)?;
    Ok(sql)
}

pub fn read_sql_file_from_path(path: &Path) -> Result<String, std::io::Error> {
    let file = File::open(path)?;
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents)?;

    Ok(contents)
}

pub fn paths_with_ext(root: impl AsRef<Path>, ext: &str) -> impl Iterator<Item = PathBuf> {
    let ext = ext.to_owned(); // capture for the 'move' below
    WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok()) // ignore entries with errors
        .filter(|e| e.file_type().is_file()) // only files
        .map(|e| e.into_path())
        .filter(move |p| p.is_extension(&ext)) // ← your trait method
}
