#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");
const repo = process.env.GITHUB_REPOSITORY || "DJGLTD/AK_Access_ctrl";
const token = process.env.GITHUB_TOKEN || process.env.GH_TOKEN || "";
const prLookupAttempts = Number(process.env.RELEASE_PR_LOOKUP_ATTEMPTS || 6);
const prLookupDelayMs = Number(process.env.RELEASE_PR_LOOKUP_DELAY_MS || 5000);

function git(args, { allowFailure = false } = {}) {
  const result = spawnSync("git", args, {
    cwd: root,
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

function run(command, args, { allowFailure = false } = {}) {
  const result = spawnSync(command, args, {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, GH_TOKEN: process.env.GH_TOKEN || token },
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    if (allowFailure) return "";
    const error = String(result.stderr || result.stdout || "").trim();
    throw new Error(error || `${command} ${args.join(" ")} failed`);
  }
  return String(result.stdout || "").trim();
}

function parsePrNumber(text) {
  const value = String(text || "");
  const patterns = [
    /Merge pull request #(\d+)\b/i,
    /\(#(\d+)\)\s*$/m,
    /pull request #(\d+)\b/i,
    /\bPR\s*#?(\d+)\b/i,
  ];
  for (const pattern of patterns) {
    const match = value.match(pattern);
    if (match) return Number(match[1]);
  }
  return null;
}

function versionFromPrNumber(number) {
  const prNumber = Number(number);
  if (!Number.isInteger(prNumber) || prNumber <= 0) {
    throw new Error(`Invalid PR number: ${number}`);
  }
  const major = Math.floor(prNumber / 100);
  const minor = Math.floor(prNumber / 10) % 10;
  const patch = prNumber % 10;
  return `${major}.${minor}.${patch}`;
}

function compareVersions(left, right) {
  const a = String(left || "").replace(/^v/, "").split(".").map(Number);
  const b = String(right || "").replace(/^v/, "").split(".").map(Number);
  for (let i = 0; i < 3; i += 1) {
    const diff = (a[i] || 0) - (b[i] || 0);
    if (diff !== 0) return diff;
  }
  return 0;
}

async function githubJson(url) {
  const headers = {
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
  };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const response = await fetch(url, {
    headers,
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`GitHub API ${response.status}: ${text || response.statusText}`);
  }
  return response.json();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function strongestPullRequest(pulls) {
  if (!Array.isArray(pulls) || !pulls.length) return null;
  return pulls
    .slice()
    .sort((a, b) => Number(b.number || 0) - Number(a.number || 0))[0];
}

async function associatedPullRequestFromCommitApi(sha) {
  if (repo && sha) {
    const pulls = await githubJson(
      `https://api.github.com/repos/${repo}/commits/${sha}/pulls`,
    );
    const pr = strongestPullRequest(pulls);
    if (pr) return pr;
  }
  return null;
}

async function mergedPullRequestFromRecentClosed(sha) {
  if (!repo || !sha) return null;
  const pulls = await githubJson(
    `https://api.github.com/repos/${repo}/pulls?state=closed&sort=updated&direction=desc&per_page=50`,
  );
  if (!Array.isArray(pulls) || !pulls.length) return null;
  return (
    pulls.find((pr) => String(pr.merge_commit_sha || "").toLowerCase() === sha.toLowerCase())
    || null
  );
}

async function associatedPullRequestFromApi(sha) {
  if (!repo || !sha) return null;
  return (await associatedPullRequestFromCommitApi(sha))
    || (await mergedPullRequestFromRecentClosed(sha));
}

async function associatedPullRequest() {
  const sha = process.env.GITHUB_SHA || git(["rev-parse", "HEAD"], { allowFailure: true });
  const attempts = Math.max(1, Number.isFinite(prLookupAttempts) ? prLookupAttempts : 1);
  const delayMs = Math.max(0, Number.isFinite(prLookupDelayMs) ? prLookupDelayMs : 0);

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const pr = await associatedPullRequestFromApi(sha);
      if (pr) return pr;
    } catch (err) {
      console.warn(`Unable to look up PR for ${sha}: ${err.message}`);
    }

    if (attempt < attempts && delayMs > 0) {
      console.log(`No associated PR found for ${sha}; retrying in ${delayMs}ms (${attempt}/${attempts}).`);
      await sleep(delayMs);
    }
  }

  const eventPath = process.env.GITHUB_EVENT_PATH;
  if (eventPath && fs.existsSync(eventPath)) {
    try {
      const event = JSON.parse(fs.readFileSync(eventPath, "utf8"));
      const messages = [
        event.head_commit?.message,
        ...(Array.isArray(event.commits) ? event.commits.map((item) => item.message) : []),
      ];
      for (const message of messages) {
        const number = parsePrNumber(message);
        if (number) return { number };
      }
    } catch (err) {
      console.warn(`Unable to inspect GitHub event payload: ${err.message}`);
    }
  }

  const commitMessage = git(["log", "-1", "--format=%B"], { allowFailure: true });
  const number = parsePrNumber(commitMessage);
  return number ? { number } : null;
}

async function pullRequestDetails(number) {
  if (!number || !repo || !token) return { number };
  try {
    return await githubJson(`https://api.github.com/repos/${repo}/pulls/${number}`);
  } catch (err) {
    console.warn(`Unable to fetch PR #${number} details: ${err.message}`);
    return { number };
  }
}

function latestVersionTag() {
  const tags = git(["tag", "--list", "v[0-9]*", "--sort=-v:refname"], {
    allowFailure: true,
  })
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
  return tags[0] || "";
}

function tagExists(tag) {
  return git(["tag", "--list", tag], { allowFailure: true }).trim() === tag;
}

function releaseExists(tag) {
  return run("gh", ["release", "view", tag, "--repo", repo], { allowFailure: true }) !== "";
}

function releaseNotes(version, pr, previousTag) {
  const lines = [`Akuvox Access Control ${version}`, ""];
  if (pr?.number) {
    const title = pr.title ? `: ${pr.title}` : "";
    const url = pr.html_url ? `\n${pr.html_url}` : "";
    lines.push(`Release generated from PR #${pr.number}${title}${url}`, "");
  }

  const range = previousTag ? `${previousTag}..HEAD` : "HEAD";
  const commits = git(["log", "--pretty=format:- %s (%h)", range], { allowFailure: true })
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter((item) => item && !/^- chore\(release\):/i.test(item));
  if (commits.length) {
    lines.push("Changes:", ...commits.slice(0, 50), "");
  }
  return lines.join("\n").trimEnd() + "\n";
}

function writeReleaseFiles(version) {
  run(process.execPath, ["scripts/set-release-version.cjs", version]);
}

function commitReleaseFiles(version, prNumber) {
  const paths = [
    "custom_components/akuvox_ac/manifest.json",
    "custom_components/akuvox_ac/const.py",
  ];
  const changed = git(["status", "--porcelain", "--", ...paths]);
  if (!changed) return false;

  git(["config", "user.name", "github-actions[bot]"]);
  git(["config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"]);
  git(["add", ...paths]);
  git([
    "commit",
    "-m",
    `chore(release): ${version} [skip ci]`,
    "-m",
    `Generated from PR #${prNumber}.`,
  ]);
  git(["push", "origin", "HEAD:main"]);
  return true;
}

function createTagAndRelease(version, pr, previousTag) {
  const tag = `v${version}`;
  if (!tagExists(tag)) {
    git(["tag", tag]);
    git(["push", "origin", tag]);
  }

  if (releaseExists(tag)) {
    console.log(`${tag} already has a GitHub release.`);
    return;
  }

  const notesPath = path.join(root, ".release-notes.md");
  fs.writeFileSync(notesPath, releaseNotes(version, pr, previousTag));
  try {
    run("gh", [
      "release",
      "create",
      tag,
      "--repo",
      repo,
      "--title",
      tag,
      "--notes-file",
      notesPath,
    ]);
  } finally {
    fs.rmSync(notesPath, { force: true });
  }
}

async function main() {
  if (process.argv[2] === "--version-from-pr") {
    console.log(versionFromPrNumber(process.argv[3]));
    return;
  }
  if (process.argv[2] === "--associated-pr-number") {
    const pr = await associatedPullRequest();
    console.log(pr?.number || "");
    return;
  }

  const associatedPr = await associatedPullRequest();
  if (!associatedPr?.number) {
    console.log("No associated PR found for this push; skipping release.");
    return;
  }

  const pr = await pullRequestDetails(associatedPr.number);
  const previousTag = latestVersionTag();
  const version = versionFromPrNumber(associatedPr.number);
  const tag = `v${version}`;

  if (tagExists(tag)) {
    console.log(`${tag} already exists; skipping release.`);
    return;
  }
  if (previousTag && compareVersions(version, previousTag) <= 0) {
    throw new Error(
      `PR #${associatedPr.number} maps to ${version}, which is not newer than ${previousTag}.`,
    );
  }

  writeReleaseFiles(version);
  commitReleaseFiles(version, associatedPr.number);
  createTagAndRelease(version, pr, previousTag);
  console.log(`Released ${tag} from PR #${associatedPr.number}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
