# Yari Lambda Functions

This `aws-lambda` folder contains one or more sub-folders each of which may
define a unique AWS Lambda function used by Yari. A sub-folder defines an AWS
Lambda function if it contains the following:

- an `index.js` file containing the code of the Lambda function
- a `package.json` file which defines the Lambda function's dependencies as
  well as a `make-package` command that when run creates an AWS Lambda
  deployment package (Zip file) containing the function's code (`index.js`)
  and dependencies (`node_modules/*`).

  Here's an example `package.json` file:

  ```json
  {
    "description": "Defines the deployment package for this AWS Lambda function.",
    "private": true,
    "main": "index.js",
    "license": "MPL-2.0",
    "scripts": {
      "make-package": "yarn install && zip -r -X function.zip . -i index.js 'node_modules/*'"
    },
    "dependencies": {
      "sanitize-filename": "^1.6.3"
    },
    "engines": {
      "node": "12.x"
    }
  }
  ```

- an `aws.yaml` file that specifies two things: the `name` of the Lambda
  function in AWS, and the AWS `region` in which the Lambda function resides

  Here's an example `aws.yml` file:

  ```yaml
  name: mdn-content-origin-request
  region: us-east-1
  ```

## Updating Lambda Functions in AWS

The command:

```sh
cd deployer
poetry run deployer update-lambda-functions
```

will discover every folder that contains a Lambda function, create a
deployment package (Zip file) for each one by running:

```sh
yarn make-package
```

and if the deployment package is different from what is already in AWS,
it will upload and publish a new version.
