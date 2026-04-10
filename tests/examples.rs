use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const FIXTURE_ROOT_PLACEHOLDER: &str = "<FIXTURE_ROOT>";

#[test]
fn example_fixtures_regenerate_expected_outputs() {
    let examples_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let fixtures =
        discover_example_fixtures(&examples_root).expect("failed to read example fixtures");

    assert!(
        !fixtures.is_empty(),
        "expected at least one example fixture under {}",
        examples_root.display()
    );

    for fixture in fixtures {
        run_fixture(&fixture);
    }
}

fn discover_example_fixtures(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut fixtures = Vec::new();
    discover_example_fixtures_inner(root, &mut fixtures)?;
    fixtures.sort();
    Ok(fixtures)
}

fn discover_example_fixtures_inner(root: &Path, fixtures: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if path.join("proj").join("mosaika.toml").is_file() && path.join("solu").is_dir() {
            fixtures.push(path.clone());
            continue;
        }
        discover_example_fixtures_inner(&path, fixtures)?;
    }
    Ok(())
}

fn run_fixture(fixture: &Path) {
    let temp_dir = TestDir::new();
    let examples_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let fixture_relative =
        fixture.strip_prefix(&examples_root).expect("fixture should stay under the examples root");
    let sandbox = temp_dir.path().join(fixture_relative);
    copy_dir_all(fixture, &sandbox).unwrap_or_else(|err| {
        panic!("failed to copy fixture {} into {}: {err}", fixture.display(), sandbox.display())
    });

    let output = Command::new(env!("CARGO_BIN_EXE_mosaika"))
        .arg("--scheme")
        .arg(sandbox.join("proj").join("mosaika.toml"))
        .arg("--force")
        .output()
        .unwrap_or_else(|err| panic!("failed to run fixture {}: {err}", fixture.display()));

    assert!(
        output.status.success(),
        "fixture {} failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        fixture.display(),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert_trees_equal(&sandbox.join("prod"), &sandbox.join("solu"), fixture);
}

fn assert_trees_equal(actual: &Path, expected: &Path, fixture: &Path) {
    let actual_fixture_root =
        actual.parent().expect("fixture prod tree should have one parent fixture directory");
    let actual_files =
        collect_relative_files(actual).unwrap_or_else(|err| panic_tree_read(actual, fixture, err));
    let expected_files = collect_relative_files(expected)
        .unwrap_or_else(|err| panic_tree_read(expected, fixture, err));

    let actual_paths: BTreeSet<_> = actual_files.keys().cloned().collect();
    let expected_paths: BTreeSet<_> = expected_files.keys().cloned().collect();
    assert_eq!(
        actual_paths,
        expected_paths,
        "fixture {} produced a different file set",
        fixture.display()
    );

    for relative_path in expected_paths {
        let actual_bytes = actual_files
            .get(&relative_path)
            .expect("actual file set should contain every expected path");
        let expected_bytes = expected_files
            .get(&relative_path)
            .expect("expected file set should contain every expected path");
        let normalized_actual = normalize_fixture_bytes(actual_bytes, actual_fixture_root);
        assert_eq!(
            &normalized_actual,
            expected_bytes,
            "fixture {} produced different contents for {}",
            fixture.display(),
            relative_path.display()
        );
    }
}

fn collect_relative_files(root: &Path) -> io::Result<BTreeMap<PathBuf, Vec<u8>>> {
    let mut files = BTreeMap::new();
    collect_relative_files_inner(root, root, &mut files)?;
    Ok(files)
}

fn collect_relative_files_inner(
    root: &Path, current: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>,
) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_relative_files_inner(root, &path, files)?;
            continue;
        }
        if !file_type.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported filesystem entry {}", path.display()),
            ));
        }
        let relative_path =
            path.strip_prefix(root).expect("walked path should stay within the tree").to_path_buf();
        files.insert(relative_path, fs::read(&path)?);
    }
    Ok(())
}

fn copy_dir_all(src: &Path, dst: &Path) -> io::Result<()> {
    let metadata = fs::metadata(src)?;
    fs::create_dir_all(dst)?;
    fs::set_permissions(dst, metadata.permissions())?;

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let target_path = dst.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_dir_all(&path, &target_path)?;
            continue;
        }
        if !file_type.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported filesystem entry {}", path.display()),
            ));
        }
        fs::copy(&path, &target_path)?;
        let metadata = fs::metadata(&path)?;
        fs::set_permissions(&target_path, metadata.permissions())?;
    }

    Ok(())
}

fn panic_tree_read(root: &Path, fixture: &Path, err: io::Error) -> ! {
    panic!("failed to read {} for fixture {}: {err}", root.display(), fixture.display())
}

fn normalize_fixture_bytes(bytes: &[u8], fixture_root: &Path) -> Vec<u8> {
    let root = fixture_root.to_string_lossy();
    if root.is_empty() {
        return bytes.to_vec();
    }

    bytes.windows(root.len()).position(|window| window == root.as_bytes()).map_or_else(
        || bytes.to_vec(),
        |_| bytes.replace(root.as_bytes(), FIXTURE_ROOT_PLACEHOLDER.as_bytes()),
    )
}

trait ByteSliceExt {
    fn replace(&self, from: &[u8], to: &[u8]) -> Vec<u8>;
}

impl ByteSliceExt for [u8] {
    fn replace(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
        if from.is_empty() {
            return self.to_vec();
        }

        let mut replaced = Vec::with_capacity(self.len());
        let mut cursor = 0;
        while let Some(offset) =
            self[cursor..].windows(from.len()).position(|window| window == from)
        {
            let start = cursor + offset;
            replaced.extend_from_slice(&self[cursor..start]);
            replaced.extend_from_slice(to);
            cursor = start + from.len();
        }
        replaced.extend_from_slice(&self[cursor..]);
        replaced
    }
}

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new() -> Self {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("mosaika-integration-test-{nonce}"));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
