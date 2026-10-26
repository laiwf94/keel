#!/usr/bin/env bash
# Checks out every commit on the current branch into a scratch worktree and runs the full gate
# there, oldest first. Prints one line per commit and exits non-zero if any commit fails; the gate
# output of a failing commit and the reports of its failing tests are kept in build/verify-commits/.
#
#   scripts/verify-commits.sh [first-commit]
set -uo pipefail
cd "$(dirname "$0")/.."

scratch=$(mktemp -d)
trap 'git worktree remove --force "$scratch/wt" >/dev/null 2>&1; rm -rf "$scratch"' EXIT

range=HEAD
[[ $# -gt 0 ]] && range="$1^..HEAD"
failed=0
total=0
for sha in $(git rev-list --reverse "$range"); do
  total=$((total + 1))
  git worktree add --detach -q "$scratch/wt" "$sha"
  start=$(date +%s)
  if (cd "$scratch/wt" && ./gradlew check --offline -q >"$scratch/gate.log" 2>&1); then
    result=pass
  else
    result=FAIL
    failed=$((failed + 1))
    mkdir -p build/verify-commits
    cp "$scratch/gate.log" "build/verify-commits/$sha.log"
    for f in "$scratch"/wt/build/test-results/test/*.xml; do
      grep -q "<failure" "$f" 2>/dev/null && cp "$f" build/verify-commits/
    done
  fi
  printf '%s %-4s %4ss  %s\n' "${sha:0:7}" "$result" "$(($(date +%s) - start))" \
    "$(git log -1 --format=%s "$sha")"
  git worktree remove --force "$scratch/wt"
done
echo "$((total - failed)) of $total commits pass the gate"
[[ $failed -eq 0 ]]
