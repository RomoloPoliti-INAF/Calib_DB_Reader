from CalibDBReader import CalibDB
from rich.console import Console


test = CalibDB("../../JANUS/Software/janus_cal_db")
print(test)
data = test.get_calib(
    "deadPixels", "2024-01-01", read_data=True, return_class=True, debug=True
)

console = Console()
console.print(data)
