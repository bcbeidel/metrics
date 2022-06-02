# metrics

dbt package to facilitate the calculation of consistent metrics across cloud warehouses

## Installation instructions

New to dbt packages? Read more about them [here](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/).

1. Include this package in your `packages.yml`
2. Run `dbt deps`
3. Include the following in your `dbt_project.yml` directly within your `vars`

```YAML
# dbt_project.yml
config-version: 2

...

vars:
  # number of days of job history to include in metrics calculation
  metrics__days_of_history: 90
...