# Acryl Business Glossary Sync Action

This action will open PRs against your repo to update your business glossary yaml file with the latest updates from your Acryl instance.

## Prerequisites

Make sure you also enable `Allow GitHub Actions to create and approve pull requests` under `Settings > Actions` in your repository.

## Usage

```yml
# Put this in .github/workflows/acryl-business-glossary-sync.yml.
name: Acryl Business Glossary Sync

on:
  schedule:
    # Example: cron expression for 5:30am and 5:30pm UTC.
    - cron: "30 5,17 * * *"
  workflow_dispatch:

permissions:
  contents: write
  pull-requests: write

jobs:
  glossary-sync:
    runs-on: ubuntu-latest

    steps:
      - name: Run glossary sync
        uses: acryldata/business-glossary-sync-action@main
        with:
          # TODO(developer): Update this with your config/credentials.
          business_glossary_file: ./business_glossary.yml
          enable_auto_id: "true"

          datahub_gms_host: https://<customer>.acryl.io/gms
          datahub_gms_token: ${{ secrets.ACRYL_GMS_TOKEN }}
```
