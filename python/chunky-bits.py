#!/usr/bin/env python3
from urllib import request
from urllib.parse import urlparse
import sys
import yaml

if len(sys.argv) > 1:
    filename = sys.argv[1];
    with open(filename) as f:
        file_ref = yaml.load(f, Loader=yaml.FullLoader)
    length = file_ref.get("length")
    for part in file_ref.get("parts"):
        for data in part.get("data"):
            location = data.get("locations")[0]
            location_url = urlparse(location)
            if location_url.scheme == "":
                with open(location, "rb") as f:
                    content = f.read()
            else:
                with request.urlopen(location) as f:
                    content = f.read()
            if length is not None:
                if len(content) > length:
                    content = content[:length]
                length -= len(content)
            sys.stdout.buffer.write(content)
else:
    print("chunky-bits.py <file>")