use std::io::Error;
use std::path::PathBuf;
use crate::config::loader::Layers;
use walkdir::WalkDir;

pub fn walk_dirs(dirs: Layers) -> Result<(), Error>{
    let current_dir = std::env::current_dir()?;

    for (name, dir) in dirs.iter() {
        let current_dir = std::env::current_dir().unwrap();
        for file in WalkDir::new(dir) {
            
        }
    }
    Ok(())

}

fn scan_dir(dir: &PathBuf) -> Result<Vec<String>, Error> {
    let mut files = Vec::new();
    for file in WalkDir::new(dir) {
        let entry = file?
            .into_path()
            .to_str()
            .unwrap()
            .to_string();
        files.push(entry);
    }
    
    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::Error;
    use std::path::PathBuf;
    use crate::directory::scan_dir;

    #[test]
    fn test_scan_dir() -> Result<(), Error >{
        let test_path = "./tmp/foundry_models/bronze";
        fs::create_dir_all(test_path)?;
        let path = PathBuf::from(test_path);
        
        let file_path = path.join("test.sql");
        File::create(&file_path)?;
        
        let files = scan_dir(&file_path)?;
        
        assert_eq!(files, vec![String::from("./tmp/foundry_models/bronze/test.sql")]);
        fs::remove_file(file_path)?;
        Ok(())
    }
}