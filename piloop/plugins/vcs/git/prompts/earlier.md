## Earlier candidates

- Before you build a mechanism from scratch, look for it in past campaigns ("Remember past campaigns" in the
  procedures). A keep in another campaign is code to take (`git -C <its path> show <commit>`). A `dead` lesson is a
  pitfall that someone already paid for.
- Every candidate's commit is in this repo. The kept lineage below lists each one's commit.
  `SELECT commit_sha FROM candidates WHERE id = <n>` gives the commit of any other.
- Use `git show <commit_sha>`, `git diff <base> <sha>` and `git cherry-pick <sha>`.
- Read a candidate's diff before you repeat its mechanism, and take what worked.
