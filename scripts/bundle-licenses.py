"""Collect release dependency licenses and the Rust library notices."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess


def release_packages(metadata):
    deps = {node["id"]: node["deps"] for node in metadata["resolve"]["nodes"]}
    visited = set()
    pending = list(metadata["workspace_members"])
    while pending:
        item = pending.pop()
        if item in visited:
            continue
        visited.add(item)
        pending.extend(edge["pkg"] for edge in deps[item]
                       if any(kind["kind"] != "dev" for kind in edge["dep_kinds"]))
    packages = (pkg for pkg in metadata["packages"] if pkg["id"] in visited and pkg["source"])
    return sorted(packages, key=lambda pkg: (pkg["name"], pkg["version"]))


def license_files(package):
    directory = Path(package["manifest_path"]).parent
    stems = ("LICENSE", "LICENCE", "COPYING", "NOTICE", "UNLICENSE")
    files = {path for path in directory.iterdir() if path.is_file()
             and any(path.name.upper() == stem or path.name.upper().startswith(
                 (f"{stem}-", f"{stem}_", f"{stem}.")) for stem in stems)}
    if package["license_file"]:
        files.add(directory / package["license_file"])
    if not files:
        raise ValueError(f"{package['name']} {package['version']} has no packaged license text")
    return sorted(files, key=lambda path: path.name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, help="Target used for the bundle build")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--locked", "--all-features",
         "--filter-platform", args.target]
    ))
    args.output.mkdir(parents=True)
    index = ["Rust library license notices appear in rust-library.html.",
             "The following registry dependencies were resolved for this bundle.", ""]
    for package in release_packages(metadata):
        name = f"{package['name']}-{package['version']}"
        output = args.output / name
        output.mkdir()
        for path in license_files(package):
            shutil.copyfile(path, output / path.name)
        index.append(f"{name}  {package['license'] or 'See packaged license text'}")
    sysroot = Path(subprocess.check_output(["rustc", "--print", "sysroot"], text=True).strip())
    notice = sysroot / "share/doc/rust/COPYRIGHT-library.html"
    shutil.copyfile(notice, args.output / "rust-library.html")
    (args.output / "INDEX.txt").write_text("\n".join(index) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
