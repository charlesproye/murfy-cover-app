import asyncio
from fleet_info import read_fleet_info as fleet_info
from other import main as other_main
from tesla import main as tesla_main


if __name__ == "__main__":

    fleet_info = asyncio.run(fleet_info())
    print(fleet_info)
    # Get the correct info model for each brand 
    asyncio.run(tesla_main(fleet_info))
    #Get the information of the model based on the excel file 
    asyncio.run(other_main(fleet_info))

