# Deployment Environments

## There are 3 environments

1. Prod - developer.mozilla.org
2. Stage - developer.allizom.org
3. Dev(s) - [main].content.dev.mdn.mozit.cloud

## Ground rules

Prod and stage are supposed to be as similar as possible.

All environments are as separate from each other as possible.

You can manually trigger a build for any environment (using the GitHub UI)
but only prod and stage build on a cron job.

## The details

We will try to commit to maintaining the (public) details for each environment in
[this spreadsheet](https://docs.google.com/spreadsheets/d/1VnnEl-iTtKYmlyN02FiEXygxZCgE4o_ZO8wSleebne4/edit?usp=sharing).

But the best source of details are in viewing the 3 GitHub Action workflows:

1. `prod-build.yml`
1. `stage-build.yml`
1. `dev-build.yml`

Note! Yes, these files are very similar in various tricks and techniques are
blatantly repeated across all three files. But the benefit is that it keeps things
simple for the benefit of security.

## Stage is for testing

The stage environment is updated most frequently. Use it for quality assurance
testing.

Unlike dev, the stage environment is actually connected to a real database
(the Kuma stage environment).

## Dev is for experimenting

Use this environment when you can't wait for the cron schedule for stage,
or if you want to try out a specific (git) branch.

The default deployment prefix is `main` which is the first word in the domain name:
`main.content.dev.mdn.mozit.cloud`. But you can deploy to the dev environment
with a custom prefix. E.g. `web-perf-experiment123` and then the URL becomes:
`web-perf-experiment123.content.dev.mdn.mozit.cloud`.

## With or without Kuma

The biggest difference between dev compared to prod & stage, is that the
backend API is faked in dev. This includes the ability to sign in.
Also, in prod & stage, the CDN (AWS CloudFront) is configured to fall back to
Kuma to render what can't be rendered as a static asset.

## Tracking Whatsdeployed

These URLs give you a comparison between what's the `master` (for Yari)
and `main` (for Content) in the git repo, compared to what's made it into
each deployment environment. This helps to answer "Has my change gone live yet?"

- [Code](https://whatsdeployed.io/s/Rzl/mdn/yari)
- [Content](https://whatsdeployed.io/s/DLi/mdn/content)
