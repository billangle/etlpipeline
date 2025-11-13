

## Getting Started with CDK



Helpful documentation:
- AWS CDK Documentation: https://docs.aws.amazon.com/cdk/api/v2/ $\textcolor{silver}{↗}$
- HelpFul Book: AWS CDK in Practice: Unleash the power of ordinary coding and streamline complex cloud applications on AWS : https://www.amazon.com/gp/product/B0BJF8PRHD/ref=ppx_yo_dt_b_search_asin_title?ie=UTF8&psc=1 $\textcolor{silver}{↗}$
- GitHub Repo from the Book: https://github.com/PacktPublishing/AWS-CDK-in-Practice $\textcolor{silver}{↗}$
- Getting Started Reference: https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html $\textcolor{silver}{↗}$

<br>

## Installing CDK

Requires NodeJS

````
npm install -g aws-cdk
````

## CDK Bootstrap

```bash
cdk bootstrap -- -c account={AWS account id} -c region={AWS region} --profile {AWS credentials}
````
Should be run from a directory that contains a valid cdk.json file.



