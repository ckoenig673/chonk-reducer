# Screenshot Standards

Product screenshots must come from a local demo environment containing synthetic data.

Current assets:

- `chonk-dashboard.png` — isolated dashboard demo with synthetic libraries and savings history

Before committing a screenshot, verify that it contains none of the following:

- Real media filenames or library paths
- Hostnames, IP addresses, usernames, or mount points
- Webhook URLs, encryption keys, tokens, or other secrets
- Production database contents or logs

Preferred initial views:

1. Dashboard with synthetic library and savings data
2. Preview results showing non-destructive candidate decisions
3. Analytics summary with synthetic historical totals
4. Settings page with all sensitive fields empty or redacted

Use PNG format, crop browser chrome unless it provides useful context, and keep the viewport wide enough for README rendering.
