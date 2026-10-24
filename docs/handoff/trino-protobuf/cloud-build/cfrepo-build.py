"""Lay out the io.confluent jars copied from the cp-schema-registry image as a Maven file repository.

Usage: python3 -I cfrepo-build.py <jars dir> <repo dir>
Jars whose embedded pom.properties name an io.confluent or org.apache.kafka artifact are copied to
<repo>/<group path>/<artifact>/<version>/ with their embedded pom and .sha1 files.
"""
import hashlib
import os
import sys
import zipfile

jars_dir, repo = sys.argv[1], sys.argv[2]
WANTED_GROUPS = ("io.confluent", "org.apache.kafka")
seen = set()
placed = []


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
    with open(path + ".sha1", "w") as f:
        f.write(hashlib.sha1(data).hexdigest())


for root, _, files in os.walk(jars_dir):
    for name in files:
        if not name.endswith(".jar") or name in seen:
            continue
        seen.add(name)
        path = os.path.join(root, name)
        try:
            z = zipfile.ZipFile(path)
        except zipfile.BadZipFile:
            continue
        props = [n for n in z.namelist() if n.startswith("META-INF/maven/") and n.endswith("/pom.properties")]
        for p in props:
            kv = {}
            for line in z.read(p).decode("utf-8", "replace").splitlines():
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    kv[k.strip()] = v.strip()
            g, a, v = kv.get("groupId"), kv.get("artifactId"), kv.get("version")
            if not (g and a and v) or not g.startswith(WANTED_GROUPS):
                continue
            if a + "-" + v + ".jar" != name:
                continue  # shaded or renamed jar; the properties describe a bundled dependency
            pom_entry = p[: -len("pom.properties")] + "pom.xml"
            base = os.path.join(repo, g.replace(".", "/"), a, v, f"{a}-{v}")
            with open(path, "rb") as f:
                write(base + ".jar", f.read())
            if pom_entry in z.namelist():
                write(base + ".pom", z.read(pom_entry))
            else:
                placed.append(f"NO POM {g}:{a}:{v}")
                continue
            placed.append(f"{g}:{a}:{v}")

print("\n".join(sorted(placed)))
