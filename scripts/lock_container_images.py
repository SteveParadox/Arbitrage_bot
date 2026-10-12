"""Resolve explicit image versions to immutable registry digests; no daemon needed."""

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import urllib.request
import urllib.parse


def resolve(image):
    name, tag = image.rsplit(":", 1)
    scope = name if "/" in name else "library/" + name
    query = urllib.parse.urlencode(
        {"service": "registry.docker.io", "scope": "repository:" + scope + ":pull"}
    )
    with urllib.request.urlopen(
        "https://auth.docker.io/token?" + query, timeout=15
    ) as response:
        token = json.load(response)["token"]
    request = urllib.request.Request(
        "https://registry-1.docker.io/v2/" + scope + "/manifests/" + tag,
        method="HEAD",
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json",
        },
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        digest = response.headers["Docker-Content-Digest"]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("invalid registry digest")
    return digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    files = [
        *root.glob("docker/Dockerfile.*"),
        root / "docker/production/compose.yml",
        root / "docker/production/monitoring.yml",
    ]
    pattern = re.compile(
        r"(?:FROM|image:)\s+([a-z0-9][a-z0-9./_-]*:[A-Za-z0-9._-]+)(?![A-Za-z0-9._-])"
    )
    names = {
        match.group(1) for path in files for match in pattern.finditer(path.read_text())
    }
    images = {name: resolve(name) for name in sorted(names)}
    (root / "docker/production/images.lock.json").write_text(
        json.dumps(
            {
                "checked_at": datetime.now(UTC).isoformat(),
                "images": images,
                "container_vulnerability_scan": False,
            },
            indent=2,
        )
        + "\n"
    )
    if args.apply:
        for path in files:
            contents = path.read_text()
            for name, digest in images.items():
                contents = re.sub(
                    re.escape(name) + r"(?:@sha256:[0-9a-f]{64})?",
                    name + "@" + digest,
                    contents,
                )
            path.write_text(contents)
    print(json.dumps({"resolved": len(images), "applied": args.apply}))


if __name__ == "__main__":
    main()
