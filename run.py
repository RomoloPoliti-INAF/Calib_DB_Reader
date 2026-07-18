from rich.console import Console

from CalibDBReader import CalibDB

test = CalibDB("../../JANUS/Software/janus_cal_db")
print(test)
data = test.get_calib("iOnF", "2024-01-01", read_data=True, debug=True)

console = Console()
console.print(data)
