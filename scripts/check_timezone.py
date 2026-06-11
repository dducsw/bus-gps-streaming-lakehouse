"""
Kiểm tra: các file sub_raw thuộc ngày 2025-03-22 (PROCESS_DATE trong trip_summary.py)
Xem phân bố hour UTC vs VN của chúng để hiểu tại sao chart hiển thị lạ.
"""
import json
import os
from datetime import datetime, timezone, timedelta

VN = timezone(timedelta(hours=7))

base_dir = 'data/HPCLAB/part2/part2'
TARGET_DATE_UTC = '2025-03-22'
TARGET_DATE_VN  = '2025-03-22'

# Scan ALL files and find those containing data for 2025-03-22
print(f"Scanning files for data on UTC date {TARGET_DATE_UTC} or VN date {TARGET_DATE_VN}...")
print()

total_utc_hours = {}
total_vn_hours  = {}
files_matched = []

all_files = sorted(f for f in os.listdir(base_dir) if f.endswith('.json'))
print(f"Total files: {len(all_files)}")

for fname in all_files:
    fpath = os.path.join(base_dir, fname)
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        continue
    
    file_has_target = False
    for obj in data:
        if obj.get('msgType') == 'MsgType_BusWayPoint':
            wp = obj.get('msgBusWayPoint', {})
            ep = wp.get('datetime')
            if ep:
                dt_utc = datetime.fromtimestamp(ep, tz=timezone.utc)
                dt_vn  = datetime.fromtimestamp(ep, tz=VN)
                # Check if this record belongs to target date (VN perspective)
                if dt_vn.date().isoformat() == TARGET_DATE_VN:
                    total_vn_hours[dt_vn.hour] = total_vn_hours.get(dt_vn.hour, 0) + 1
                    total_utc_hours[dt_utc.hour] = total_utc_hours.get(dt_utc.hour, 0) + 1
                    file_has_target = True
    
    if file_has_target:
        files_matched.append(fname)
        print(f"  MATCH: {fname}")

print()
print(f"Files containing VN date {TARGET_DATE_VN}: {files_matched}")
print()
print("Hour distribution for that VN date:")
print("  Hour(VN) | count   | Hour(UTC when stored)")
for h in range(24):
    vc = total_vn_hours.get(h, 0)
    if vc > 0:
        # UTC hour is VN - 7
        utc_h = (h - 7) % 24
        bar = '#' * (vc // 1000)
        print(f"    {h:02d}     | {vc:7d} | UTC={utc_h:02d}  {bar[:40]}")

print()
print("Top VN hours:", sorted(total_vn_hours.items(), key=lambda x: -x[1])[:8])
print("Top UTC hours:", sorted(total_utc_hours.items(), key=lambda x: -x[1])[:8])
