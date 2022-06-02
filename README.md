# metrics

dbt package to facilitate the calculation of consistent metrics across cloud warehouses

## Assumptions

This package assumes that you have created models that in a structure similar to the following example file(s):

- [examples/seeds/user_mapping.csv](examples/seeds/user_mapping.csv): Identifies which queries are considered 'users' rather than programmatic accounts

## Installation

1. Include this package in your `packages.yml`
2. Run `dbt deps`
3. Include the following in your `dbt_project.yml` directly within your `vars`

```YAML
# dbt_project.yml
config-version: 2

...

vars:
  # location of table that maps user_name to boolean 'is_user_account'
  metrics__user_mapping_table: "{{ ref('metrics__user_mapping') }}"
  # number of days of job history to include in metrics calculation
  metrics__days_of_history: 90
```

4. Execute `dbt run --full-refresh` for the first iteration – the models will get built as part of your run!
5. Execute `dbt run` for future runs, leveraging the built-in [incremental models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models).
