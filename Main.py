import pandas as pd
import os
import sys
from threading import Thread, Lock

buffSize = int(sys.argv[1])
productores = int(sys.argv[2])
consumerFile = sys.argv[3]
alternancia = sys.argv[4]

Queque = [buffSize]
