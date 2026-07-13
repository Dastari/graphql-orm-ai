use std::fs;
use std::path::{Path, PathBuf};

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).expect("source directory should be readable") {
            let path = entry.expect("source entry should be readable").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
    files
}

#[test]
fn crate_source_has_no_direct_database_dependency() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest =
        fs::read_to_string(root.join("Cargo.toml")).expect("manifest should be readable");
    assert!(!manifest.contains("sqlx ="));
    assert!(!manifest.contains("tiberius ="));

    for path in rust_files(&root.join("src")) {
        let source = fs::read_to_string(&path).expect("Rust source should be readable");
        for forbidden in ["sqlx::", "tiberius::", "DATABASE_URL", "TEST_DATABASE_URL"] {
            assert!(
                !source.contains(forbidden),
                "{} contains forbidden boundary reference {forbidden}",
                path.display()
            );
        }
    }
}
