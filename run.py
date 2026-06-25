from CalibDBReader import CalibDB
from rich.console import Console


test = CalibDB("../../JANUS/Software/janus_cal_db")
print(test)
data = test.get_calib(
    "radCorrection", "2024-01-01", read_data=True,filter=12, debug=True
)

console = Console()
console.print(data)
