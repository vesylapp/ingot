# List recipes.
default:
    @just --list

# Bump the version (--patch by default, or --minor / --major), build, then commit, tag and push
bump level="--patch":
    #!/usr/bin/env bash
    # The pushed vX.Y.Z tag triggers the Docker workflow, which publishes the versioned image.
    set -euo pipefail

    case "{{ level }}" in
      --major|--minor|--patch) ;;
      *) echo "usage: just bump [--major|--minor|--patch]" >&2; exit 1 ;;
    esac

    if [[ -n "$(git status --porcelain)" ]]; then
      echo "working tree is not clean; commit or stash first" >&2
      exit 1
    fi

    current=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
    IFS=. read -r major minor patch <<<"$current"
    case "{{ level }}" in
      --major) major=$((major + 1)); minor=0; patch=0 ;;
      --minor) minor=$((minor + 1)); patch=0 ;;
      --patch) patch=$((patch + 1)) ;;
    esac
    next="$major.$minor.$patch"
    tag="v$next"

    git fetch --tags --quiet origin
    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
      echo "tag $tag already exists" >&2
      exit 1
    fi

    echo "bumping $current -> $next"
    # Only the [package] version, which comes first in Cargo.toml.
    awk -v cur="version = \"$current\"" -v nv="version = \"$next\"" \
      '!done && $0 == cur { print nv; done = 1; next } { print }' Cargo.toml >Cargo.toml.tmp
    mv Cargo.toml.tmp Cargo.toml

    # Builds the release binary and refreshes Cargo.lock with the new version.
    cargo build --release

    git add Cargo.toml Cargo.lock
    git commit -m "chore: release $tag"
    git tag -a "$tag" -m "$tag"
    git push --atomic origin HEAD "refs/tags/$tag"
    echo "pushed $tag"
