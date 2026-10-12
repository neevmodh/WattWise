"""Hadoop Streaming: per-minute observed kWh, aggregated within each mapper."""
import sys
from collections import defaultdict
from datetime import datetime
totals = defaultdict(lambda: [0.0, 0])
for line in sys.stdin:
    fields = line.strip().split(';')
    if len(fields) != 9 or fields[0] == 'Date': continue
    try:
        date = datetime.strptime(fields[0], '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError: continue
    totals[date]  # Retain dates where every power observation is missing.
    try: energy = float(fields[2]) / 60
    except ValueError: continue
    totals[date][0] += energy; totals[date][1] += 1
for date, (energy, count) in totals.items(): print(f'{date}\t{energy:.12f}\t{count}')
