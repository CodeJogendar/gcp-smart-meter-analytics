"""Generate smart-meter sample data.

Outputs
  meters.csv                       20-row meter dimension
  landing/meter_readings_part*.csv 4 files, 400 data rows in total, as delivered by an upstream system
  ../sql/02_seed_meters.sql        INSERT statements for the dimension

The 400 reading rows are made up of:
  388 unique readings  (384 for known meters, 4 for an unknown meter)
    6 exact duplicates (re-delivered rows)
    6 invalid rows     (missing id, negative / non-numeric kWh, impossible date, wrong column count, bad voltage)
The unique readings include 8 injected consumption spikes and 16 missing meter-days (data gaps).
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(11)
OUT = Path(__file__).parent
LANDING = OUT / "landing"
START = date(2026, 10, 28)
N_DAYS = 20
N_GAPS, N_SPIKES, N_DUPLICATES = 16, 8, 6
HEADER = ["reading_id", "meter_id", "reading_date", "kwh_consumed", "voltage", "read_source", "reading_quality"]

CITIES = [("Hyderabad", "Telangana"), ("Bengaluru", "Karnataka"), ("Chennai", "Tamil Nadu"),
          ("Pune", "Maharashtra"), ("Visakhapatnam", "Andhra Pradesh")]
TYPES = ["Residential"] * 12 + ["Commercial"] * 5 + ["Industrial"] * 3
TARIFF = {"Residential": "Domestic", "Commercial": "Commercial", "Industrial": "HT-Industrial"}
BASE_KWH = {"Residential": (8, 16), "Commercial": (60, 120), "Industrial": (300, 600)}
LOAD_KW = {"Residential": (3, 8), "Commercial": (20, 60), "Industrial": (200, 500)}


def make_meters():
    meters = []
    for i, ctype in enumerate(TYPES, start=1):
        city, state = CITIES[(i - 1) % len(CITIES)]
        meters.append({
            "meter_id": f"MTR-{i:03d}", "customer_type": ctype, "city": city, "state": state,
            "tariff_plan": TARIFF[ctype], "feeder_id": f"FDR-{random.randint(1, 6):02d}",
            "sanctioned_load_kw": random.randint(*LOAD_KW[ctype]),
            "install_date": (date(2022, 1, 1) + timedelta(days=random.randint(0, 1200))).isoformat(),
        })
    return meters


def csv_line(values):
    from io import StringIO
    buf = StringIO()
    csv.writer(buf, lineterminator="").writerow(values)
    return buf.getvalue()


def main():
    meters = make_meters()
    rows = []  # (reading_date, meter_id, fields)
    counter = iter(range(1, 100_000))
    for meter in meters:
        base = random.uniform(*BASE_KWH[meter["customer_type"]])
        for day in range(N_DAYS):
            d = START + timedelta(days=day)
            weekend = d.weekday() >= 5
            factor = (1.15 if weekend else 1.0) if meter["customer_type"] == "Residential" else (0.7 if weekend else 1.0)
            kwh = round(base * factor * random.uniform(0.94, 1.06), 2)
            rows.append({"reading_id": f"RD-{next(counter):05d}", "meter_id": meter["meter_id"],
                         "reading_date": d.isoformat(), "kwh_consumed": kwh,
                         "voltage": round(random.uniform(224, 238), 1),
                         "read_source": "MANUAL" if random.random() < 0.05 else "AMI",
                         "reading_quality": "ESTIMATED" if random.random() < 0.08 else "OK"})

    candidates = [r for r in rows if r["reading_date"] >= (START + timedelta(days=10)).isoformat()]
    spikes = random.sample(candidates, N_SPIKES)
    for r in spikes:
        r["kwh_consumed"] = round(r["kwh_consumed"] * random.uniform(3.8, 5.0), 2)
    spike_ids = {r["reading_id"] for r in spikes}
    gap_pool = [r for r in rows if r["reading_id"] not in spike_ids]
    gap_ids = {r["reading_id"] for r in random.sample(gap_pool, N_GAPS)}
    rows = [r for r in rows if r["reading_id"] not in gap_ids]

    for i in range(4):  # readings from a meter that is not in the dimension
        d = START + timedelta(days=random.randint(0, N_DAYS - 1))
        rows.append({"reading_id": f"RD-{next(counter):05d}", "meter_id": "MTR-099",
                     "reading_date": d.isoformat(), "kwh_consumed": round(random.uniform(8, 14), 2),
                     "voltage": 230.0, "read_source": "AMI", "reading_quality": "OK"})

    rows.sort(key=lambda r: (r["reading_date"], r["meter_id"]))
    lines = [csv_line([r[c] for c in HEADER]) for r in rows]
    lines += random.sample(lines, N_DUPLICATES)
    random.shuffle(lines)

    bad = [
        "RD-BAD-1,,2026-11-02,12.5,230.1,AMI,OK",
        "RD-BAD-2,MTR-003,2026-11-03,-4.2,229.5,AMI,OK",
        "RD-BAD-3,MTR-004,2026-11-04,abc,231.0,AMI,OK",
        "RD-BAD-4,MTR-005,2026-02-30,10.1,228.9,AMI,OK",
        "RD-BAD-5,MTR-006,2026-11-05,9.8",
        "RD-BAD-6,MTR-007,2026-11-06,11.0,999.0,AMI,OK",
    ]
    for line in bad:
        lines.insert(random.randint(0, len(lines)), line)
    assert len(lines) == 400, len(lines)

    LANDING.mkdir(exist_ok=True)
    for old in LANDING.glob("*.csv"):
        old.unlink()
    for part in range(4):
        chunk = lines[part * 100:(part + 1) * 100]
        (LANDING / f"meter_readings_part{part + 1}.csv").write_text(
            ",".join(HEADER) + "\n" + "\n".join(chunk) + "\n", encoding="utf-8")

    fields = list(meters[0])
    with open(OUT / "meters.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(meters)

    values = ",\n  ".join(
        "('{meter_id}', '{customer_type}', '{city}', '{state}', '{tariff_plan}', '{feeder_id}', "
        "{sanctioned_load_kw}, DATE '{install_date}')".format(**m) for m in meters)
    sql = ("-- Seed the meters dimension (generated by data/generate_data.py)\n"
           "INSERT INTO `${PROJECT_ID}.meters_raw.meters`\n"
           "  (meter_id, customer_type, city, state, tariff_plan, feeder_id, sanctioned_load_kw, install_date)\n"
           f"VALUES\n  {values};\n")
    (OUT.parent / "sql" / "02_seed_meters.sql").write_text(sql, encoding="utf-8")
    print(f"Wrote {len(lines)} reading rows in 4 files and {len(meters)} meters")


if __name__ == "__main__":
    main()
