# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.


## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check [existing open](https://github.com/awslabs/aws-java-nio-spi-for-s3/issues), 
or [recently closed](https://github.com/awslabs/aws-java-nio-spi-for-s3/issues?utf8=%E2%9C%93&q=is%3Aissue%20is%3Aclosed%20), 
issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* Environment
  * Java version
  * OS version
  * Location of this extension JAR (or if it was used to compile another application)
  * IAM S3 permissions of the role used (mask sensitive information if any)
  * Bucket ACL (mask sensitive information if any)
* Steps to reproduce the error
* Expected result
* Actual result
* AWS region(s) where the issue was observed and region of the bucket being read from


## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Use language features compatible with Java 11 and ensure the code will compile with the latest release of that version
4. Ensure unit tests cover your change and demonstrate expected behavior
5. Ensure unit tests do NOT require AWS credentials or S3 connectivity by using Mocks for any `S3Client` or `S3AsyncClient`. Remember, unit tests test this library and not the functionality of S3.
6. Run `./gradlew check` to ensure local tests pass and test coverage reports are produced.
This will also check for API/ABI compatibility using the [rev-api gradle plugin](https://github.com/palantir/gradle-revapi/tree/develop).
In case there is a justified breaking change, you can [accept](https://github.com/palantir/gradle-revapi/tree/develop#accepting-breaks) them.
Please mention the change and reason when opening the PR, so that maintainers consider this when versioning.
7. Ensure test coverage is not degraded. Report locations can be found [here](./README.md#testing).
8. Send us a pull request, answering any default questions in the pull request interface.
9. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).


## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the 
default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), 
looking at any ['help wanted'](https://github.com/awslabs/aws-java-nio-spi-for-s3/labels/help%20wanted) issues is a great place to start.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via 
our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). 
Please do **not** create a public Github issue.


## Licensing
See the [LICENSE](https://github.com/awslabs/aws-java-nio-spi-for-s3/blob/main/LICENSE) file for our 
project's licensing. We will ask you to confirm the licensing of your contribution.

We may ask you to sign a [Contributor License Agreement (CLA)](http://en.wikipedia.org/wiki/Contributor_License_Agreement) 
for larger changes.
