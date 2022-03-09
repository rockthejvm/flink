# The official repository for the Rock the JVM Flink course for Scala developers

This repository contains the code we wrote during  [Rock the JVM's Flink course](https://rockthejvm.com/course/flink). Unless explicitly mentioned, the code in this repository is exactly what was caught on camera.

## How to install

- install [IntelliJ IDEA](https://jetbrains.com/idea)
- install [Docker](https://www.docker.com/products/docker-desktop) 
- either clone the repo or download as zip
- open with IntelliJ as an SBT project
- (optionally) in the `docker` directory, navigate to each subdir (except for `flink`) and run `docker-compose up`

### How to start

Clone this repository and checkout the `start` tag by running the following in the repo folder:

```
git checkout start
```

To see the final code, run:

```
git checkout master
```

### How to run an intermediate state

The repository was built while recording the lectures. Prior to each lecture, I tagged each commit so you can easily go back to an earlier state of the repo!

The tags are as follows:

- `start`
- `1.1-scala-recap`
- `2.1-essential-streams`
- `2.2-essential-streams-exercise`
- `2.3-essential-streams-explicit`
- `2.5-window-functions`
- `2.6-window-functions-part-2`
- `2.7-window-functions-exercise`
- `2.8-time-based-transformations`
- `2.9-triggers`
- `2.10-multiple-streams`
- `2.11-partitions`
- `3.2-rich-functions`
- `3.3-keyed-state`
- `3.4-keyed-state-2`
- `3.5-broadcast-state`
- `3.6-checkpoint`
- `4.1-kafka`
- `4.2-jdbc`
- `4.3-cassandra`
- `4.4-source-functions`
- `4.5-custom-sinks`
- `4.6-side-outputs`

When you watch a lecture, you can `git checkout` the appropriate tag and the repo will go back to the exact code I had when I started the lecture.

### For questions or suggestions

If you have changes to suggest to this repo, either
- submit a GitHub issue
- tell me in the course Q/A forum
- submit a pull request!
