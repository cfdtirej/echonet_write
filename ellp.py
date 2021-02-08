import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any, Iterator

import numpy as np


# listの値の型変換
def list_val_type_conv(data: List[str]) -> List[Any]:
    numbers = {str(i) for i in range(10)}
    result = []
    for value in data:
        if (type(value) is float) or (type(value) is int):
            result.append(value)
            continue
        str_set = set()
        for string in value:
            str_set.add(str(string))
        try:
            result.append(float(value))
        except ValueError:
            if value in ['True', 'False', 'true', 'false']:
                result.append(bool(value))
            elif str_set - numbers:
                try:
                    if '-' in value:
                        ts = datetime.strptime(value + '+0900', '%Y-%m-%d %H:%M:%S.%f%z').isoformat()
                        result.append(ts)
                    elif '/' in value:
                        ts = datetime.strptime(value + '+0900', '%Y/%m/%d %H:%M:%S.%f%z').isoformat()
                        result.append(ts)
                except ValueError:
                    result.append(value)
            elif value == '':
                result.append(np.nan)
            else:
                pass
    return result


# echonetliteのcsvをlineProtocolに変換
def elcsv_lp_generator(csvfile: str) -> Iterator[List[Dict[str, Any]]]:
    with open(csvfile) as f:
        reader = csv.reader(f)
        header = next(reader)
        header.append('ch_total')
        for record in reader:
            record = list_val_type_conv(record)
            ch_total = 0
            for i in record[1:]:
                if (type(i) == float) and (i != np.nan):
                    ch_total += i
            record.append(ch_total)
            line_protocol = [{
                'measurement': 'echonet',
                'time': record[0],
                'fields': {k: v for k, v in zip(header[1:], record[1:])}
            }]
            yield line_protocol


def el_socket_lp(columns: List[str], record: List[Any]) -> List[Dict[str, Any]]:
    columns.append('ch_total')
    record = list_val_type_conv(record)
    ch_total = 0
    for num in record[1:]:
        if (type(num) is float) or (type(num) is int) and (num != np.nan):
            ch_total += num
    record.append(ch_total)
    line_protocol = [{
        'measurement': 'echonet',
        'time': record[0],
        'fields': {k: v for k, v in zip(columns[1:], record[1:])}
    }]
    return line_protocol


