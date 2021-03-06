#!/usr/bin/env python3
from urllib import request
from urllib.parse import urlparse
import hashlib
import sys
import yaml

if len(sys.argv) > 1:
    # Read file reference metadata
    filename = sys.argv[1];
    with open(filename) as f:
        file_ref = yaml.load(f, Loader=yaml.FullLoader)

    length = file_ref.get("length")
    
    for part in file_ref.get("parts"):
        # Only read the data chunks
        # Erasure coding not implemented yet
        for data in part.get("data"):
            # Only check the first location
            # Parse it as a URL
            location = data.get("locations")[0]
            location_url = urlparse(location)

            # Read the URL or file path
            if location_url.scheme == "":
                with open(location, "rb") as f:
                    content = f.read()
            else:
                with request.urlopen(location) as f:
                    content = f.read()

            # Check the content hash
            known_hash = data.get("sha256")
            content_hash = hashlib.sha256(content).hexdigest()
            if known_hash != content_hash:
                print("%s != %s" % (known_hash, content_hash), file=sys.stderr)

            # Truncate the chunk if needed
            if length is not None:
                if len(content) > length:
                    content = content[:length]
                length -= len(content)

            sys.stdout.buffer.write(content)
else:
    print("chunky-bits.py <file>")