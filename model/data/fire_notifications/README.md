# Fire Notifications/TextMeFires data selection

Auth info is here: https://docs.google.com/spreadsheets/d/1ccoJ0Y3I2bgB4UCpsFz3rXO_3uwHp3ot7ZfkIJhaKuc/edit?gid=0#gid=0 (WD login required)

Archives are available for 14 days, so files must be copied if they are to be used.

## Selecting useful streams

**Selecting candidate streams**

All streams are mono mp3, with dead air trimmed.

**Selecting candidate files**

```sh
python fetch_fn_archives_day.py
# outputs a CSV file with URLs
```
