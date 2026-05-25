#!/usr/bin/env node

const { spawnSync } = require("node:child_process");

function git(args, { allowFailure = false } = {}) {
  const result = spawnSync("git", args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    if (allowFailure) return "";
    const error = String(result.stderr || result.stdout || "").trim();
    throw new Error(error || `git ${args.join(" ")} failed`);
  }
  return String(result.stdout || "").trim();
}

function latestVersionTag() {
  return git(
    ["describe", "--tags", "--abbrev=0", "--match", "v[0-9]*"],
    { allowFailure: true },
  );
}

function commitsSinceLatestTag() {
  const tag = latestVersionTag();
  const range = tag ? `${tag}..HEAD` : "HEAD";
  const raw = git(["log", "--format=%H%x1f%s%x1f%B%x1e", range], {
    allowFailure: true,
  });
  if (!raw) return [];

  return raw
    .split("\x1e")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const [hash = "", subject = "", body = ""] = entry.split("\x1f");
      return { hash: hash.trim(), subject: subject.trim(), body: body.trim() };
    });
}

function isReleaseCommit(commit) {
  const text = `${commit.subject}\n${commit.body}`;
  return /(^|\n)chore\(release\):\s+\d+\.\d+\.\d+/i.test(text);
}

function requestsMinorRelease(commit) {
  const text = `${commit.subject}\n${commit.body}`;
  return (
    /(^|\n)BREAKING CHANGE:/i.test(text)
    || /(^|\n)(feat|feature|major)(\([^)]+\))?!?:/i.test(text)
    || /\[(minor|feature|major)\]/i.test(text)
    || /#(minor|feature|major)\b/i.test(text)
  );
}

const commits = commitsSinceLatestTag().filter((commit) => !isReleaseCommit(commit));

if (commits.length === 0) {
  process.exit(0);
}

if (commits.some(requestsMinorRelease)) {
  console.log("minor");
  process.exit(0);
}

console.log("patch");
