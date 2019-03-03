from src import CSVCatalog

import time
import json
def test():
    b = []
    b.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    CSVCatalog.ColumnDefinition("playerID", "text", True).to_json()
    print("ok")
    return b
test()