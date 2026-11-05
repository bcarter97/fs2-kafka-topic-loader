# Contributing

Contributions are welcomed!

When contributing to this repository, please first discuss the change you wish to make via GitHub issue before making a change. This saves everyone from wasted effort in the event that the proposed changes need some adjustment before they are ready for submission.

## Pull Request Process

1. Fork the repo, push your commits to a branch of your fork, open the PR.
2. Make sure you update the README.md where relevant.
3. Project maintainers will squash-merge Pull Requests once they are happy. There may be one or more cycles of feedback on the PR before they are satisfied.

## Build and run tests locally

The software is written in [Scala](https://scala-lang.org/) and is built with [SBT](http://www.scala-sbt.org/).

The project is built and released for Scala 3. Run `sbt ciBuild` to lint and run both the unit and integration tests, exactly as CI does. The tests start Kafka using [Testcontainers](https://testcontainers.com/), so Docker must be running.

## Versioning and compatibility

Releases follow [early semantic versioning](https://www.scala-lang.org/blog/2021/02/16/preventing-version-conflicts-with-versionscheme.html#early-semver-and-sbt-version-policy). [`sbt-version-policy`](https://github.com/scalacenter/sbt-version-policy) checks every pull request against the previous release, using the compatibility level declared for the next release in `versionPolicyIntention` in [`build.sbt`](./build.sbt). A pull request that intentionally breaks compatibility should also set `versionPolicyIntention` to `Compatibility.None`.

| Next release                 | Before 1.0        | From 1.0          | `versionPolicyIntention`                  |
|------------------------------|-------------------|-------------------|-------------------------------------------|
| Breaking                     | `0.2.3` → `0.3.0` | `1.2.3` → `2.0.0` | `Compatibility.None`                      |
| Binary compatible            | `0.2.3` → `0.2.4` | `1.2.3` → `1.3.0` | `Compatibility.BinaryCompatible`          |
| Binary and source compatible | `0.2.3` → `0.2.4` | `1.2.3` → `1.2.4` | `Compatibility.BinaryAndSourceCompatible` |

For maintainers:

1. Before tagging a release, check that `versionPolicyIntention` matches the version being released.
2. After releasing, reset `versionPolicyIntention` to `Compatibility.BinaryCompatible`.

## Performing a release (for project maintainers)

Automated publishing to Maven Central is not set up yet. CI currently only builds and tests the project. The build
already includes [`sbt-ci-release`](https://github.com/sbt/sbt-ci-release) for when it is.

## Contributor Code of Conduct

As contributors and maintainers of this project, and in the interest of fostering an open and welcoming community, we pledge to respect all people who contribute through reporting issues, posting feature requests, updating documentation, submitting pull requests or patches, and other activities.

We are committed to making participation in this project a harassment-free experience for everyone, regardless of level of experience, gender, gender identity and expression, sexual orientation, disability, personal appearance, body size, race, ethnicity, age, religion, or nationality.

Examples of unacceptable behavior by participants include:

- The use of sexualized language or imagery
- Personal attacks
- Trolling or insulting/derogatory comments
- Public or private harassment
- Publishing other's private information, such as physical or electronic addresses, without explicit permission
- Other unethical or unprofessional conduct.

Project maintainers have the right and responsibility to remove, edit, or reject comments, commits, code, wiki edits, issues, and other contributions that are not aligned to this Code of Conduct. By adopting this Code of Conduct, project maintainers commit themselves to fairly and consistently applying these principles to every aspect of managing this project. Project maintainers who do not follow or enforce the Code of Conduct may be permanently removed from the project team.

This code of conduct applies both within project spaces and in public spaces when an individual is representing the project or its community.

Instances of abusive, harassing, or otherwise unacceptable behavior may be reported by opening an issue or contacting one or more of the project maintainers.

This Code of Conduct is adapted from the [Contributor Covenant](http://contributor-covenant.org), version 1.2.0, available at [http://contributor-covenant.org/version/1/2/0/](http://contributor-covenant.org/version/1/2/0/)
