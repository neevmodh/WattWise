# Dataset access

The raw text file is 132,960,755 bytes and is intentionally excluded from Git. The repository includes aggregate results and the code needed to reproduce them.

Source: [UCI Individual Household Electric Power Consumption](https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption), by Georges Hebrail and Alice Berard. Dataset DOI: [10.24432/C58K54](https://doi.org/10.24432/C58K54). UCI lists the dataset under CC BY 4.0; retain attribution when redistributing it.

After installing the project environment:

```bash
.venv/bin/python scripts/fetch_dataset.py
```

By default, the utility saves `household_power_consumption.txt` beside the WattWise repository, where the pipeline expects it. It verifies the exact source SHA-256 and refuses to overwrite a different existing file.

Expected hash:

```text
4259c9d7ece5dbee9ab8d53682baac68d791c864f0f64a52b4043cb3b90894b7
```
